#!/usr/bin/python
# This file is part of Ansible
#
# Ansible is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Ansible is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Ansible.  If not, see <http://www.gnu.org/licenses/>.

DOCUMENTATION = '''

        """Creates a new virtual server instance.

        #:param int cpus: The number of virtual CPUs to include in the instance.
        #:param int memory: The amount of RAM to order in [1024,2048,4096,6144,8192,12288,16384,32768,49152,65536]
        #:param bool hourly: Flag to indicate if this server should be billed
                            hourly (default) or monthly.
        #:param string hostname: The hostname to use for the new server.
        #:param string domain: The domain to use for the new server.
        #:param bool local_disk: Flag to indicate if this should be a local disk
                                (default) or a SAN disk.
        #:param string datacenter: The short name of the data center in which
                                  the VS should reside.
        #:param string os_code: The operating system to use. Cannot be specified
                               if image_id is specified.
        #:param int image_id: The ID of the image to load onto the server.
                             Cannot be specified if os_code is specified.
        #:param bool dedicated: Flag to indicate if this should be housed on a
                               dedicated or shared host (default). This will
                               incur a fee on your account.
        #:param int public_vlan: The ID of the public VLAN on which you want
                                this VS placed.
        #:param int private_vlan: The ID of the private VLAN on which you want
                                 this VS placed.
        #:param list disks: A list of disk capacities for this server.
        :param string post_uri: The URI of the post-install script to run
                                after reload
        #:param bool private: If true, the VS will be provisioned only with
                             access to the private network. Defaults to false
        #:param list ssh_keys: The SSH keys to add to the root user
        #:param int nic_speed: The port speed to set
        #:param string tags: tags to set on the VS as a comma separated list


        AWS to SoftLayer concept map
        security_group -> firewall
        route53 -> dns
        instance_types -> (cpus, memory, nic_speed)
        EBS -> SAN
        user_data -> post_uri
        (Region, zone, placement_group) -> datacenter
        tenancy -> dedicated
        vpc_subnet_id ~ -> private_vlan


        """
---
module: sl
short_description: create, terminate, start or stop an instance in sl
description:
    - Creates or terminates sl instances. Wraps SoftLayer.managers.vs functionality. 'slcli vs create-options' to see all option values
version_added: "2.0"
options:
  hostname:
    version_added: "2.0"
    description:
      - the hostname to use for the new server
    required: true
    default: null
    aliases: []
  domain:
    version_added: "2.0"
    description:
      - the domain to use for the new server.  See sl_dns.py.
    required: true
    default: null
    aliases: []
  ssh_keys:
    version_added: "2.0"
    description:
      - comma separated list of names or ids of uploaded key pairs to add to root on the instance (see sl_key.py)
    required: false
    default: null
    aliases: ['key_names']
  cpus:
    version_added: "2.0"
    description:
      - the number of virtual CPUs in [1,2,4,8] for private plus [12,16] for standard instances
    required: false
    default: 1
    aliases: []
  memory:
    version_added: "2.0"
    description:
      - the amount of RAM in [1024,2048,4096,6144,8192,12288,16384,32768,49152,65536]
    required: false
    default: 1
    aliases: []
  hourly:
    version_added: "2.0"
    description:
      - Boolean flag to indicate if this server should be billed hourly (default) or monthly
    required: false
    default: true
    aliases: []
  dedicated:
    version_added: "2.0"
    description:
      - Boolean flag to indicate if this server should be billed hourly (default) or monthly
    required: false
    default: false
    aliases: [tenancy]
  os_code:
    version_added: "2.0"
    description:
       - the operating system to use in [UBUNTU_LATEST, WIN_LATEST, REDHAT_LATEST, etc]. Either os_code or image.
    required: false
    default: null
    aliases: []
  image:
    version_added: "2.0"
    description:
       - the image to use. Only one of os_code and image can be used. See sl_img.py module.
    required: false
    default: null
    aliases: [image_name]
  disks:
    version_added: "2.0"
    description:
      - a list of volume dicts, each containing device name and optionally ephemeral id or snapshot id. Size and type (and number of iops for io device type) must be specified for a new volume or a root volume, and may be passed for a snapshot volume. For any volume, a volume size less than 1 will be interpreted as a request not to create the volume.
    required: false
    default: null
    aliases: [volumes]
  local_disk:
    version_added: "2.0"
    description:
      - Flag to indicate if this should be a local disk (default) or a SAN disk
    required: false
    default: true
  nic_speed:
    version_added: "2.0"
    description:
      - the port speed to set in Mbps [ 10,100,1000 ]
    required: false
    default: 10
    aliases: []
  post_uri:
    version_added: "2.0"
    description:
      - The URI of the post-install script to run after reload
    required: false
    default: null
    aliases: [user_data_uri]
  tags:
    version_added: "2.0"
    description:
      - a hash/dictionary of tags to add to the new instance; '{"key":"value"}' and '{"key":"value","key":"value"}'
    required: false
    default: null
    aliases: [instance_tags]
  public_vlan:
    version_added: "2.0"
    description:
      - the ID of the public VLAN on which you want this VS placed
    required: false
    default: null
    aliases: []
  private_vlan:
    version_added: "2.0"
    description:
      - the ID of the private VLAN on which you want this VS placed
    required: false
    default: null
    aliases: []
  private:
    version_added: "2.0"
    description:
      - Boolean. If true, the VS will be provisioned only with access to the private network. Defaults to false
    required: false
    default: false
    aliases: []
  datacenter:
    version_added: "2.0"
    description:
      - datacenter to host the instance, in [ dal01,dal05,dal06,dal09,... many more, see create-options ]
    required: false
    default: dal01
    aliases: []
  exact_count:
    version_added: "2.0"
    description:
      - An integer value which indicates how many instances that match the 'count_tag' parameter should be running. Instances are either created or terminated based on this value. 
    required: false
    default: null
    aliases: []
  count_tag:
    version_added: "2.0"
    description:
      - Used with 'exact_count' to determine how many nodes based on a specific tag criteria should be running.  This can be expressed in multiple ways and is shown in the EXAMPLES section.  For instance, one can request 25 servers that are tagged with "class=webserver".  
    required: false
    default: null
    aliases: []
  count:
    version_added: "2.0"
    description:
      - number of instances to launch
    required: false
    default: 1
    aliases: []
  wait:
    version_added: "2.0"
    description:
      - wait for the instance to be 'running' before returning.  Does not wait for SSH, see 'wait_for' example for details.
    required: false
    default: "no"
    choices: [ "yes", "no" ]
    aliases: []
  wait_timeout:
    version_added: "2.0"
    description:
      - how long before wait gives up, in seconds
    default: 300
    aliases: []
  state:
    version_added: "2.0"
    description:
      - create or terminate instances
    required: false
    default: 'present'
    aliases: []
    choices: ['present', 'absent', 'running', 'stopped']

author: Kesten Broughton
extends_documentation_fragment: softlayer
'''

EXAMPLES = '''
# Note: These examples do not set authentication details, see the AWS Guide for details.

# Basic provisioning example
- sl:
    ssh_keys: ['mykey']
    instance_type: t2.micro
    image: ami-123456
    wait: yes
    group: webserver
    count: 3
    private_vlan: subnet-29e63245
    assign_public_ip: yes

# Advanced example with tagging and CloudWatch
- sl:
    ssh_keys: ['mykey']
    group: databases
    instance_type: t2.micro
    image: ami-123456
    wait: yes
    wait_timeout: 500
    count: 5
    instance_tags: 
       db: postgres
    monitoring: yes
    private_vlan: subnet-29e63245
    assign_public_ip: yes

# Single instance with additional IOPS volume from snapshot and volume delete on termination
- sl:
    ssh_keys: ['mykey']
    group: webserver
    instance_type: c3.medium
    image: ami-123456
    wait: yes
    wait_timeout: 500
    volumes:
      - device_name: /dev/sdb
        snapshot: snap-abcdef12
        device_type: io1
        iops: 1000
        volume_size: 100
        delete_on_termination: true
    monitoring: yes
    private_vlan: subnet-29e63245
    assign_public_ip: yes

# Multiple groups example
- sl:
    ssh_keys: ['mykey']
    group: ['databases', 'internal-services', 'sshable', 'and-so-forth']
    instance_type: m1.large
    image: ami-6e649707
    wait: yes
    wait_timeout: 500
    count: 5
    instance_tags: 
        db: postgres
    monitoring: yes
    private_vlan: subnet-29e63245
    assign_public_ip: yes

# Multiple instances with additional volume from snapshot
- sl:
    ssh_keys: ['mykey']
    group: webserver
    instance_type: m1.large
    image: ami-6e649707
    wait: yes
    wait_timeout: 500
    count: 5
    volumes:
    - device_name: /dev/sdb
      snapshot: snap-abcdef12
      volume_size: 10
    monitoring: yes
    private_vlan: subnet-29e63245
    assign_public_ip: yes

# Dedicated tenancy example
- local_action:
    module: sl
    assign_public_ip: yes
    group_id: sg-1dc53f72
    ssh_keys: ['mykey']
    image: ami-6e649707
    instance_type: m1.small
    tenancy: dedicated
    private_vlan: subnet-29e63245
    wait: yes

# Spot instance example
- sl:
    spot_price: 0.24
    spot_wait_timeout: 600
    keypair: ['mykey']
    group_id: sg-1dc53f72
    instance_type: m1.small
    image: ami-6e649707
    wait: yes
    private_vlan: subnet-29e63245
    assign_public_ip: yes

# Launch instances, runs some tasks
# and then terminate them

- name: Create a sandbox instance
  hosts: localhost
  gather_facts: False
  vars:
    ssh_keys: my_keypair
    instance_type: m1.small
    security_group: my_securitygroup
    image: my_ami_id
    region: us-east-1
  tasks:
    - name: Launch instance
      sl:
         ssh_keys: "{{ keypair }}"
         group: "{{ security_group }}"
         instance_type: "{{ instance_type }}"
         image: "{{ image }}"
         wait: true
         region: "{{ region }}"
         private_vlan: subnet-29e63245
         assign_public_ip: yes
      register: sl
    - name: Add new instance to host group
      add_host: hostname={{ item.public_ip }} groupname=launched
      with_items: sl.instances
    - name: Wait for SSH to come up
      wait_for: host={{ item.public_dns_name }} port=22 delay=60 timeout=320 state=started
      with_items: sl.instances

- name: Configure instance(s)
  hosts: launched
  sudo: True
  gather_facts: True
  roles:
    - my_awesome_role
    - my_awesome_test

- name: Terminate instances
  hosts: localhost
  connection: local
  tasks:
    - name: Terminate instances that were previously launched
      sl:
        state: 'absent'
        instance_ids: '{{ sl.instance_ids }}'

# Start a few existing instances, run some tasks
# and stop the instances

- name: Start sandbox instances
  hosts: localhost
  gather_facts: false
  connection: local
  vars:
    instance_ids:
      - 'i-xxxxxx'
      - 'i-xxxxxx'
      - 'i-xxxxxx'
    region: us-east-1
  tasks:
    - name: Start the sandbox instances
      sl:
        instance_ids: '{{ instance_ids }}'
        region: '{{ region }}'
        state: running
        wait: True
        private_vlan: subnet-29e63245
        assign_public_ip: yes
  role:
    - do_neat_stuff
    - do_more_neat_stuff

- name: Stop sandbox instances
  hosts: localhost
  gather_facts: false
  connection: local
  vars:
    instance_ids:
      - 'i-xxxxxx'
      - 'i-xxxxxx'
      - 'i-xxxxxx'
    region: us-east-1
  tasks:
    - name: Stop the sandbox instances
      sl:
        instance_ids: '{{ instance_ids }}'
        region: '{{ region }}'
        state: stopped
        wait: True
        private_vlan: subnet-29e63245
        assign_public_ip: yes

#
# Enforce that 5 instances with a tag "foo" are running
# (Highly recommended!)
#

- sl:
    ssh_keys: ['mykey']
    instance_type: c1.medium
    image: ami-40603AD1
    wait: yes
    group: webserver
    instance_tags:
        foo: bar
    exact_count: 5
    count_tag: foo
    private_vlan: subnet-29e63245
    assign_public_ip: yes

#
# Enforce that 5 running instances named "database" with a "dbtype" of "postgres"
#

- sl:
    ssh_keys: ['mykey']
    instance_type: c1.medium
    image: ami-40603AD1
    wait: yes
    group: webserver
    instance_tags: 
        Name: database
        dbtype: postgres
    exact_count: 5
    count_tag: 
        Name: database
        dbtype: postgres
    private_vlan: subnet-29e63245
    assign_public_ip: yes

#
# count_tag complex argument examples
#

    # instances with tag foo
    count_tag:
        foo:

    # instances with tag foo=bar
    count_tag:
        foo: bar

    # instances with tags foo=bar & baz
    count_tag:
        foo: bar
        baz:

    # instances with tags foo & bar & baz=bang
    count_tag:
        - foo
        - bar
        - baz: bang

'''

DISK_SIZES = [10,100,1000]

import sys
import time
from ast import literal_eval

####################### TODO move this to sl_utils.py
#from sl_utils import get_tags
def get_instance_name_id_pairs(sl, regex=None):
    results = sl.list_instances(hostname=regex)
    pairs = []
    for r in results:
        pairs.append((r['hostname'], r['id']))
    return pairs

def get_tags(sl, instance_id):
    tag_refs = sl.get_instance(instance_id)['tagReferences']
    tags = []
    for ref in tag_refs:
        tags.append(ref['tag']['name'])
#########################

try:
    import SoftLayer.managers.vs
    #from SoftLayer.exceptions import SoftLayerAPIError
    import SoftLayer

except ImportError:
    print "failed=True msg='SoftLayer required for this module.  pip install --upgrade softlayer'"

def find_running_instances_by_count_tag(module, sl, count_tag, params={}):

    # get instances that match tag(s) and are running
    running_instances = get_instances(module, sl, state="running", params=params)

    filtered_instances = []
    for item in running_instances:
        if set.intersection(set(count_tag), set(get_tags(sl,item['id']))) != set():
            filtered_instances.append(item)
                
    return filtered_instances

def get_instances(module, sl, state=None, params={}):
    """

    :param module: ansible module object
    :param sl: softlayer manager
    :param params: dict of items compatiblie with vs.list_instances()
    :return: list of instance info dicts
    """

    # TODO: filters do not work with tags that have underscores
    # Unlike AWS, tags are simple lists of strings, not dicts

    results = sl.list_instances(params=params)
    if state:
        filtered_results = []
        for item in results:
            if hasattr(item, 'status'):
                if item['status']['keyName'] == state:
                    filtered_results.append(item)
        return filtered_results

    return results

def get_running_instances_by_ids(module, sl, ids):
    """

    :param module: ansible module object
    :param sl: softlayer manager
    :param params: dict of items compatiblie with vs.list_instances()
    :return: list of instance info dicts
    """
    filtered_instances = []
    for id in ids:
        instance = sl.get_instance(id)
        if instance:
            if instance['status']['keyName'] == "ACTIVE":
                filtered_instances.append(instance)
        else:
            raise Exception(str("Cannot get info for instance with id {}".format(id)))
    return filtered_instances

def get_instance_info(inst):
    """
    Retrieves instance information from an instance
    ID and returns it as a dictionary
    """
    instance_info = {'id': inst.id,
                     'private_ip': inst.primaryIpAddress,
                     'private_dns_name': inst.primaryBackendIpAddress,
                     'public_ip': inst.ip_address,
                     'dns_name': inst.dns_name,
                     'public_dns_name': inst.public_dns_name,
                     'state_code': inst.state_code,
                     'image_id': inst.image_id,
                     'datacenter': inst.datacenter,
                     'ssh_keys': inst.ssh_keys,
                     'launch_time': inst.launch_time,
                     'root_device_type': inst.root_device_type,
                     'root_device_name': inst.root_device_name,
                     'state': inst.state,
                     'hypervisor': inst.hypervisor}

    return instance_info

def enforce_count(module, sl):

    exact_count = module.params.get('exact_count')
    count_tag = module.params.get('count_tag')
    datacenter = module.params.get('datacenter')

    # fail here if the exact count was specified without filtering
    # on a tag, as this may lead to a undesired removal of instances
    if exact_count and count_tag is None:
        module.fail_json(msg="you must use the 'count_tag' option with exact_count")

    instances = find_running_instances_by_count_tag(module, sl, count_tag)

    changed = None
    checkmode = False
    instance_dict_array = []
    changed_instance_ids = None

    if len(instances) == exact_count:
        changed = False
    elif len(instances) < exact_count:
        changed = True
        to_create = exact_count - len(instances)
        if not checkmode:
            (instance_dict_array, changed_instance_ids, changed) \
                = create_instances(module, sl, override_count=to_create)

            for inst in instance_dict_array:
                instances.append(inst)
    elif len(instances) > exact_count:
        changed = True
        to_remove = len(instances) - exact_count
        if not checkmode:
            all_instance_ids = sorted([ x.id for x in instances ])
            remove_ids = all_instance_ids[0:to_remove]

            instances = [ x for x in instances if x.id not in remove_ids]

            (changed, instance_dict_array, changed_instance_ids) \
                = terminate_instances(module, sl, remove_ids)
            terminated_list = []
            for inst in instance_dict_array:
                inst['state'] = "terminated"
                terminated_list.append(inst)
            instance_dict_array = terminated_list            
   
    # ensure all instances are dictionaries 
    all_instances = []
    for inst in instances:
        if type(inst) is not dict:
            inst = get_instance_info(inst)
        all_instances.append(inst)            

    return (all_instances, instance_dict_array, changed_instance_ids, changed)
    
def _get_key_ids(module, sl):
    """
    :param module:
    :param sl:
    :return:
    """
    ssh_key_mgr = SoftLayer.managers.SshKeyManager(sl)
    ssh_keys = module.params.get('ssh_keys')
    all_keys = ssh_key_mgr.list_keys()
    ssh_key_ids = []
    missing_keys = []
    # Todo pathalogical case of key with valid id for a string name
    for key in ssh_keys:
        for k in all_keys:
            # first try to find the keys as ids
            if key.isdigit():
                if k.id == key:
                   ssh_key_ids.append(k.id)
            # if not, try to lookup the key ids assuming we are given their names
            else:
                if k.label == key:
                    ssh_key_ids.append(k.id)
        else:
            missing_keys.append(k)
    if len(missing_keys) > 0:
        module.fail_json(msg = str("create_instances - some keys were not found: {}".format(missing_keys)))
    return ssh_key_ids

def _get_sl_params(module, sl):
    """
    extract subset of ansible.params used in sl.manager commands
    :param module: ansible module
    :return:
    """

    cpus = module.params.get('cpus')
    memory = module.params.get('memory')
    dedicated = module.params.get('dedicated')
    image_id = module.params.get('image_id')
    os_code = module.params.get('os_code')
    tags = module.params.get('tags')
    public_vlan = module.params.get('public_vlan')
    private_vlan = module.params.get('private_vlan')
    private = module.params.get('private')
    disks = module.params.get('disks')
    local_disk = module.params.get('local_disk')
    nic_speed = module.params.get('nic_speed')
    user_data = module.params.get('user_data')

    # Transform ansible module input to format expected by sl.manager modules
    ssh_key_ids = _get_key_ids(module, sl)

    params = {}
    if ssh_key_ids:
        params['ssh_keys'] = ssh_key_ids
    if cpus:
        params['cpus'] = cpus
    if memory:
        params['memory'] = memory
    if image_id:
        params['image_id'] = image_id
    if os_code:
        params['os_code'] = os_code
    if user_data:
        params['user_data'] = user_data
    if dedicated:
        params['dedicated'] = dedicated
    if public_vlan:
        params['public_vlan'] = public_vlan
    if private_vlan:
        params['private_vlan'] = private_vlan
    if private:
        params['private'] = private
    if tags:
        params['tags'] = tags
    if disks:
        for disk in disks:
            if not disk in DISK_SIZES:
                module.fail_json(msg = str("disk size must be in {}".format(DISK_SIZES))
                                      + str("got {}.format".disks))
        params['disks'] = disks
    if local_disk:
        params['local_disk'] = local_disk
    if nic_speed:
        params['nic_speed'] = nic_speed
    if user_data:
        module.fail_json(msg = str("user_data not implemented yet"))


def create_instances(module, sl, override_count=None):
    """
    Creates new instances

    We consider instances not in the running state to be manually administered and do
    not include them in counts for asserting a specific number of servers with a tag profile.

    module : AnsibleModule object
    sl: authenticated sl client object

    Returns:
        A list of dictionaries with instance information
        about the instances that were launched
    """

    count = 0
    exact_count = module.params.get('exact_count')
    if override_count:
        count = override_count
    else:
        count = module.params.get('count')
    wait = module.params.get('wait')
    wait_timeout = int(module.params.get('wait_timeout'))

    count_tag = module.params.get('count_tag')

    requested_instances = []
    running_instances =[]
    ids = []
    if count == 0:
        changed = False
    else:
        changed = True
        try:
            vs_mgr = SoftLayer.managers.vs(sl)

            params = _get_sl_params(module, sl)
            vs_mgr.verify_create_instances(**params)
            requested_instances = vs_mgr.create_instance(**params)
            ids = [ x['id'] for x in requested_instances]

        #except boto.exception.BotoServerError, e:
        except SoftLayer.SoftLayerAPIError as e:
            module.fail_json(msg = "Instance creation failed => %s: %s" % (e.error_code, e.error_message))

        # wait here until the instances are up
        num_running = 0
        wait_timeout = time.time() + wait_timeout
        while wait_timeout > time.time() and num_running < count:
            try:
                running_instances = get_running_instances_by_ids(module, sl, ids)
                num_running = len(running_instances)
            #except boto.exception.BotoServerError, e:
            except SoftLayer.SoftLayerAPIError, e:
                if e.error_code == 'InvalidInstanceID.NotFound':
                    time.sleep(1)
                    continue
                else:
                    module.fail_json(msg = "sl.get_instance failed." + e.message)

            if wait and len(running_instances) < len(ids):
                time.sleep(5)
            else:
                break

        if wait and wait_timeout <= time.time():
            # waiting took too long
            module.fail_json(msg = "wait for instances running timeout on %s" % time.asctime())

    instance_dict_array = []
    for inst in running_instances:
        d = get_instance_info(inst)
        instance_dict_array.append(d)

    return (instance_dict_array, ids, changed)

def get_all_instances(sl, instance_ids):
    instances = []
    for instance_id in instance_ids:
        instances.append(sl.get_instance(instance_id))
    return instances

def terminate_instances(module, sl, instance_ids):
    """
    Terminates a list of instances

    module: Ansible module object
    sl: authenticated sl connection object
    termination_list: a list of instances to terminate in the form of
      [ {id: <inst-id>}, ..]

    Returns a dictionary of instance information
    about the instances terminated.

    If the instance to be terminated is running
    "changed" will be set to False.

    """

    # Whether to wait for termination to complete before returning
    wait = module.params.get('wait')
    wait_timeout = int(module.params.get('wait_timeout'))

    changed = False
    instance_dict_array = []

    if not isinstance(instance_ids, list) or len(instance_ids) < 1:
        module.fail_json(msg='instance_ids should be a list of instances, aborting')

    terminated_instance_ids = []
    for res in get_all_instances(sl, instance_ids):
        for inst in res.instances:
            if inst.state == 'running' or inst.state == 'stopped':
                terminated_instance_ids.append(inst.id)
                instance_dict_array.append(get_instance_info(inst))
                try:
                    sl.terminate_instances([inst.id])
                except SoftLayerAPIError, e:
                    module.fail_json(msg='Unable to terminate instance {0}, error: {1}'.format(inst.id, e))
                changed = True

    # wait here until the instances are 'terminated'
    if wait:
        num_terminated = 0
        wait_timeout = time.time() + wait_timeout
        while wait_timeout > time.time() and num_terminated < len(terminated_instance_ids):
            response = sl.get_all_instances( \
                instance_ids=terminated_instance_ids, \
                filters={'instance-state-name':'terminated'})
            try:
                num_terminated = len(response.pop().instances)
            except Exception, e:
                # got a bad response of some sort, possibly due to
                # stale/cached data. Wait a second and then try again
                time.sleep(1)
                continue

            if num_terminated < len(terminated_instance_ids):
                time.sleep(5)

        # waiting took too long
        if wait_timeout < time.time() and num_terminated < len(terminated_instance_ids):
            module.fail_json(msg = "wait for instance termination timeout on %s" % time.asctime())

    return (changed, instance_dict_array, terminated_instance_ids)


def startstop_instances(module, sl, instance_ids, state):
    """
    Starts or stops a list of existing instances

    module: Ansible module object
    sl: authenticated sl connection object
    instance_ids: The list of instances to start in the form of
      [ {id: <inst-id>}, ..]
    state: Intended state ("running" or "stopped")

    Returns a dictionary of instance information
    about the instances started/stopped.

    If the instance was not able to change state,
    "changed" will be set to False.

    """
    
    wait = module.params.get('wait')
    wait_timeout = int(module.params.get('wait_timeout'))
    changed = False
    instance_dict_array = []
    
    if not isinstance(instance_ids, list) or len(instance_ids) < 1:
        module.fail_json(msg='instance_ids should be a list of instances, aborting')

    # Check that our instances are not in the state we want to take them to
    # and change them to our desired state
    running_instances_array = []
    for res in get_all_instances(sl, instance_ids):
        for inst in res.instances:
           if inst.state != state:
               instance_dict_array.append(get_instance_info(inst))
               try:
                   if state == 'running':
                       inst.start()
                   else:
                       inst.stop()
               except SoftLayerAPIError, e:
                   module.fail_json(msg='Unable to change state for instance {0}, error: {1}'.format(inst.id, e))
               changed = True

    ## Wait for all the instances to finish starting or stopping
    wait_timeout = time.time() + wait_timeout
    while wait and wait_timeout > time.time():
        instance_dict_array = []
        matched_instances = []
        for res in get_all_instances(sl, instance_ids):
            for i in res.instances:
                if i.state == state:
                    instance_dict_array.append(get_instance_info(i))
                    matched_instances.append(i)
        if len(matched_instances) < len(instance_ids):
            time.sleep(5)
        else:
            break

    if wait and wait_timeout <= time.time():
        # waiting took too long
        module.fail_json(msg = "wait for instances running timeout on %s" % time.asctime())

    return (changed, instance_dict_array, instance_ids)


def main():
    argument_spec = sl_argument_spec()
    argument_spec.update(dict(
            instance_ids = dict(type='list'),
            hostname = dict(),
            domain = dict(),
            ssh_keys = dict(aliases = ['key_names']),
            cpus = dict(type='int', default='1'),
            memory = dict(type='int', default='1024'),
            hourly = dict(type='bool', default=True),
            dedicated = dict(default=False, aliases='tenancy'),
            os_code = dict(),
            image = dict(),
            disks = dict(type='list', aliases = ['volumes']),
            local_disk = dict(type='bool', default=True),
            nic_speed = dict(type='int', default='10'),
            post_uri = dict(aliases=['user_data_uri']),
            instance_tags = dict(type='dict'),
            public_vlan = dict(),
            private_vlan = dict(),
            private = dict(type='bool',default=False),
            datacenter = dict(default="dal01"),
            exact_count = dict(type='int', default=None),
            count_tag = dict(),
            count = dict(type='int', default='1'),
            wait = dict(type='bool', default=False),
            wait_timeout = dict(default=300),
            state = dict(default='present'),
        )
    )

    module = AnsibleModule(
        argument_spec=argument_spec,
        mutually_exclusive = [
                                ['exact_count', 'count'],
                                ['exact_count', 'state'],
                                ['exact_count', 'instance_ids'],
                                ['os_code', 'image']
                             ],
    )

    sl = sl_connect(module)

    tagged_instances = [] 

    state = module.params.get('state')

    if state == 'absent':
        instance_ids = module.params.get('instance_ids')
        if not isinstance(instance_ids, list):
            module.fail_json(msg='termination_list needs to be a list of instances to terminate')

        (changed, instance_dict_array, new_instance_ids) = terminate_instances(module, sl, instance_ids)

    elif state in ('running', 'stopped'):
        instance_ids = module.params.get('instance_ids')
        if not isinstance(instance_ids, list):
            module.fail_json(msg='running list needs to be a list of instances to run: %s' % instance_ids)

        (changed, instance_dict_array, new_instance_ids) = startstop_instances(module, sl, instance_ids, state)

    elif state == 'present':
        # Changed is always set to true when provisioning new instances
        if not module.params.get('image') and not module.params.get('os_code'):
            module.fail_json(msg='either image or os_code is required for new instance')

        if module.params.get('exact_count') is None:
            (instance_dict_array, new_instance_ids, changed) = create_instances(module, sl)
        else:
            (tagged_instances, instance_dict_array, new_instance_ids, changed) = enforce_count(module, sl)

    module.exit_json(changed=changed, instance_ids=new_instance_ids, instances=instance_dict_array, tagged_instances=tagged_instances)

# import module snippets
from ansible.module_utils.basic import *
from ansible.module_utils.sl import *

main()
