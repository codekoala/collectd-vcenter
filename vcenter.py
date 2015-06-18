# collectd-vcenter - vcenter.py
#
# Author : Loic Lambiel @ exoscale
# Contributor : Josh VanderLinden
# Description : This is a collectd python module to gather stats from Vmware
#               vcenter

import logging
import ssl
import time

from pysphere import VIServer

try:
    import collectd
    COLLECTD_ENABLED = True
except ImportError:
    COLLECTD_ENABLED = False

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    # Legacy Python that doesn't verify HTTPS certificates by default
    pass
else:
    # Handle target environment that doesn't support HTTPS verification
    ssl._create_default_https_context = _create_unverified_https_context

MB = 1024 ** 2
HOST_STATUS = ('green', 'gray', 'yellow', 'red')


class Collector(object):

    def __init__(self, vcenters, username=None, password=None,
                 verbose=False):
        """
        Configuration to poll a vCenter cluster for performance data.

        :param list vcenters:
            A list of one or more vCenter server IPs or hostnames.
        :param str username:
            The username to use to authenticate against the vCenter cluster.
        :param str password:
            The password associated with the specified user.
        :param bool verbose: (optional)
            Whether to enable verbose logging.
        :param int sleep_time: (optional)
            Number of seconds to wait between polls.

        """

        self.vcenters = vcenters
        self.username = username
        self.password = password
        self.verbose = verbose

        if COLLECTD_ENABLED:
            self.log = logging.getLogger()
            self.log.addHandler(CollectdHandler(self.verbose))
        else:
            logging.basicConfig(level=logging.DEBUG)
            self.log = logging.getLogger()

    def poll(self):
        """
        Collect current performance information.

        """

        stats = {}
        for vcenter in self.vcenters:
            stats[vcenter] = self.poll_vcenter(vcenter)

        return stats

    def poll_vcenter(self, vcenter):
        """
        Open a connection to the specified vCenter server and begin gathering
        information about its datastores, datacenters, clusters, and hosts.

        :param str vcenter:
            The hostname or IP of a vCenter server.

        :returns:
            A dictionary containing information about the current state of
            objects managed by the specified vCenter.

        """

        self.log.debug('polling %s@%s' % (self.username, vcenter))
        server = VIServer()

        try:
            server.connect(vcenter, self.username, self.password)
        except:
            self.log.exception('Failed to connect to %s' % (vcenter,))
            return {}

        stats = {
            'datastore': {},
            'datacenter': {},
        }

        for obj, name in server.get_datastores().items():
            ds_stats = self.poll_datastore(server, obj, name)
            stats['datastore'][name] = ds_stats

        datacenters = server.get_datacenters()
        for obj, name in datacenters.items():
            dc_stats = self.poll_datacenter(server, obj, name)
            stats['datacenter'][name] = dc_stats

        return stats

    def poll_datastore(self, server, obj, name):
        """
        Gather metrics about a specific datastore.

        :param VIServer server:
            A valid connection to a vCenter server.
        :param MOR obj:
            Managed object for the datastore.
        :param str name:
            Name of the datastore.

        :returns:
            A dictionary with three keys: capacity, free, and usage. The
            capacity and free space are measured in megabytes while the usage
            is a percentage.

        """

        capacity = free = usage = 0

        try:
            self.log.debug('query datastore %s' % (name,))
            props = server._retrieve_properties_traversal(property_names=[
                'name',
                'summary.capacity',
                'summary.freeSpace',
            ], from_node=obj, obj_type='Datastore')

            for ps in props:
                for prop in ps.PropSet:
                    pn, pv = prop.Name, prop.Val
                    if pn == 'summary.capacity':
                        capacity = pv / MB
                    elif pn == 'summary.freeSpace':
                        free = pv / MB
        except:
            self.log.exception('Failed to get datastore metrics')

        if capacity > 0:
            usage = (capacity - free) / float(capacity) * 100

        return {
            'capacity': capacity,
            'free': free,
            'usage': usage,
        }

    def poll_datacenter(self, server, obj, name):
        """
        Gather metrics about a specific datacenter.

        :param VIServer server:
            A valid connection to a vCenter server.
        :param MOR obj:
            Managed object for the datacenter.
        :param str name:
            Name of the datacenter.

        :returns:
            A dictionary with several keys describing the current state of the
            datacenter. This dictionary includes information about each cluster
            and host that is part of the specified datacenter.

        """

        if '.' in name:
            name = name.split('.')[0]

        stats = self._poll_group('datacenter', server, obj, name)

        cluster_host_stats = self._poll_group('cluster', server, obj, name)
        for key, value in cluster_host_stats.items():
            if key not in stats:
                stats[key] = value
            elif isinstance(stats[key], dict):
                for c_key, c_value in value.items():
                    stats[key][c_key] = c_value
            else:
                if 'percent' in key:
                    stats[key] = (stats[key] + value) / 2
                else:
                    stats[key] += value

        return stats

    def poll_cluster(self, server, obj, name):
        """
        Gather metrics about a specific cluster.

        :param VIServer server:
            A valid connection to a vCenter server.
        :param MOR obj:
            Managed object for the cluster.
        :param str name:
            Name of the cluster.

        :returns:
            A dictionary with several keys describing the current state of the
            cluster. This dictionary includes information about each host that
            is part of the specified cluster.

        """

        return self._poll_group('cluster', server, obj, name)

    def _poll_group(self, group_type, server, obj, name):
        """
        Generic metrics gathering for datacenters and clusters.

        :param VIServer server:
            A valid connection to a vCenter server.
        :param MOR obj:
            Managed object for a datacenter or cluster.
        :param str name:
            Name of a datacenter or cluster.

        :returns:
            A dictionary with several keys describing the current state of the
            datacenter/cluster. This dictionary includes information about each
            cluster and/or host that is part of the specified object.

        """

        # change collection behavior based on the type of group we're dealing
        # with
        if group_type == 'datacenter':
            # find each cluster in the datacenter
            find_children = server.get_clusters
            poll_child = self.poll_cluster
            child_type = 'cluster'
        elif group_type == 'cluster':
            # find each host in the datacenter or cluster
            find_children = server.get_clusters
            find_children = server.get_hosts
            poll_child = self.poll_host
            child_type = 'host'

        self.log.debug('start querying %s: %s' % (group_type, name))
        children = find_children(obj)
        self.log.debug('finish querying %s: %s' % (group_type, name))

        # initialize some metrics
        cpu_total = cpu_usage = cpu_percent = 0
        mem_total = mem_usage = mem_percent = 0
        vms_total = vms_running = vms_stopped = 0
        child_stats = {}

        # iterate over each child node in this object group
        for child_obj, child_name in children.items():
            stats = poll_child(server, child_obj, child_name)
            child_stats[child_name] = stats

            # aggregate data from each child to the top level
            cpu_total += stats['cpu_total']
            cpu_usage += stats['cpu_usage']

            mem_total += stats['mem_total']
            mem_usage += stats['mem_usage']

            vms_total += stats['vms_total']
            vms_running += stats['vms_running']
            vms_stopped += stats['vms_stopped']

        # recalculate percentages
        if cpu_total > 0:
            cpu_percent = cpu_usage / float(cpu_total) * 100

        if mem_total > 0:
            mem_percent = mem_usage / float(mem_total) * 100

        # return the current metrics for this group
        group_stats = {
            'cpu_total': cpu_total,
            'cpu_usage': cpu_usage,
            'cpu_percent': cpu_percent,
            'mem_total': mem_total,
            'mem_usage': mem_usage,
            'mem_percent': mem_percent,
            'vms_total': vms_total,
            'vms_running': vms_running,
            'vms_stopped': vms_stopped,
            child_type: child_stats,
        }

        return group_stats

    def poll_host(self, server, obj, name):
        """
        Gather metrics about a specific host.

        :param VIServer server:
            A valid connection to a vCenter server.
        :param MOR obj:
            Managed object for the host.
        :param str name:
            Name of the host.

        :returns:
            A dictionary with several keys describing the current state of the
            host, including CPU, memory, and virtual machine information.

        """

        self.log.debug('found host: %s' % (name,))

        status = 0
        cpu_total = cpu_usage = cpu_percent = cpu_count = cpu_mhz_per_core = 0
        mem_total = mem_usage = mem_percent = 0
        vms_total = vms_running = vms_stopped = 0

        if '.' in name and name.count('.') != 3:
            name = name.split('.')[0]

        props = server._retrieve_properties_traversal(property_names=[
            'name',
            'summary.overallStatus',
            'summary.quickStats.overallMemoryUsage',
            'summary.quickStats.overallCpuUsage',
            'summary.hardware.memorySize',
            'summary.hardware.numCpuCores',
            'summary.hardware.cpuMhz',
        ], from_node=obj, obj_type='HostSystem')

        for prop_set in props:
            for prop in prop_set.PropSet:
                pn, pv = prop.Name, prop.Val

                if pn == 'summary.overallStatus':
                    status = HOST_STATUS.index(pv)
                elif pn == 'summary.quickStats.overallMemoryUsage':
                    mem_usage = pv
                elif pn == 'summary.quickStats.overallCpuUsage':
                    cpu_usage = pv
                elif pn == 'summary.hardware.memorySize':
                    mem_total = pv / MB
                elif pn == 'summary.hardware.numCpuCores':
                    cpu_count = pv
                elif pn == 'summary.hardware.cpuMhz':
                    cpu_mhz_per_core = pv

        vms_total = len(server.get_registered_vms(obj))
        vms_running = len(server.get_registered_vms(obj, status='poweredOn'))
        vms_stopped = len(server.get_registered_vms(obj, status='poweredOff'))

        cpu_total = cpu_count * cpu_mhz_per_core
        cpu_percent = cpu_usage / float(cpu_total) * 100
        mem_percent = mem_usage / float(mem_total) * 100

        stats = {
            'status': status,
            'cpu_total': cpu_total,
            'cpu_usage': cpu_usage,
            'cpu_percent': cpu_percent,
            'cpu_count': cpu_count,
            'mem_total': mem_total,
            'mem_usage': mem_usage,
            'mem_percent': mem_percent,
            'vms_total': vms_total,
            'vms_running': vms_running,
            'vms_stopped': vms_stopped,
        }

        return stats


class CollectdCollector(Collector):
    """
    Handle dispatching statistics to collectd.

    """

    NAME = 'vCenter'

    def __init__(self, *args, **kwargs):
        super(CollectdCollector, self).__init__(*args, **kwargs)

        self.sleep_time = kwargs.get('sleep_time', 20)

    def configure(self, conf):
        """
        Callback to configure the plugin based on collectd's settings.

        """

        for node in conf.children:
            key = node.key
            val = node.values[0]
            if key == 'Vcenter':
                self.vcenters = val.split()
            elif key == 'Username':
                self.username = val
            elif key == 'Password':
                self.password = val
            elif key == 'Verbose':
                self.verbose = bool(val)
            elif key == 'Sleep':
                self.sleep_time = int(val)
            else:
                self.log.warn('Unknown config key: %s' % (key,))

    def read(self):
        """
        Callback to send data back to collectd.

        """

        self.log.debug('Beginning read callback')
        info = self.poll()

        if not info:
            self.log.warn('No data received')
            return

        def dispatch_host(name, data):
            """
            Helper to reduce duplication

            """

            for key, value in data.items():
                self.dispatch(name, 'host_%s' % (key,), name, value)

        # report information for all vCenter servers
        for vcenter, data in info.items():
            # report datastore information
            for ds_name, ds_data in data['datastore'].items():
                for key, value in ds_data.items():
                    self.dispatch(vcenter, 'ds_%s' % (key,), ds_name, value)

            # report datacenter information
            for dc_name, dc_data in data['datacenter'].items():
                # extract any cluster and host information for later processing
                clusters = dc_data.pop('cluster', {})
                hosts = dc_data.pop('host', {})

                for key, value in dc_data.items():
                    self.dispatch(vcenter, 'dc_%s' % (key,), dc_name, value)

                # report cluster information
                for c_name, c_data in clusters.items():
                    c_hosts = c_data.pop('host', {})

                    for key, value in c_data.items():
                        o_type = 'cluster_%s' % (key,)
                        self.dispatch(dc_name, o_type, c_name, value)

                    for ch_name, ch_data in c_hosts.items():
                        dispatch_host(ch_name, ch_data)

                # report host information
                for h_name, h_data in hosts.items():
                    dispatch_host(h_name, h_data)

        time.sleep(self.sleep_time)

    def dispatch(self, host, obj_type, obj_instance, value):
        """
        Helper to clean up metric sending.

        :param str host:
            The name of the host to which the metric belongs.
        :param str obj_type:
            The type of metric to report.
        :param str obj_instance:
            An instance to associate with the metric.
        :param int value:
            The value of the metric.

        """

        val = collectd.Values(type='gauge', plugin=self.NAME, host=host)
        val.type_instance = obj_type
        val.plugin_instance = obj_instance
        val.values = [value]
        val.dispatch()


class CollectdHandler(logging.Handler):
    """
    Expose collectd logger using standard Python logging.

    """

    def __init__(self, verbose=False, *args, **kwargs):
        self.verbose = verbose
        super(CollectdHandler, self).__init__(*args, **kwargs)

        if COLLECTD_ENABLED:
            self._handler_map = {
                logging.CRITICAL: collectd.error,
                logging.ERROR: collectd.error,
                logging.WARN: collectd.warning,
                logging.INFO: collectd.info,
                logging.DEBUG: collectd.info,
            }

    def emit(self, record):
        if not COLLECTD_ENABLED:
            return

        if record.level == logging.DEBUG and not self.verbose:
            return

        handler = self._handler_map[record.level]
        handler(record.getMessage())


if COLLECTD_ENABLED:
    instance = CollectdCollector([])

    collectd.register_config(instance.configure)
    collectd.register_read(instance.read)
