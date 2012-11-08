#
# Cookbook Name:: threecrowd_scrapi
# Recipe:: default
#
# Copyright 2010, 3Crowd
#

service_name = 'scrapi'

include_recipe 'java'

cassy_server_names = Array.new
cassy_servers = search(:node, "chef_environment:#{node.chef_environment} AND recipes:threecrowd_cassandra")

cassy_servers.each do |server|
  cassy_server_names << server['fqdn'] + ":9160"
end

cassy_server_names.sort!

print "found the following cassy servers: "
p cassy_server_names.join(",")

if cassy_server_names.empty?
  Chef::Log.error("there are no cassy servers to configure scrapi with")
end

node.threecrowd[:scrapi][:cassy_servers] = cassy_server_names
node.threecrowd[:scrapi][:keyspace] = node.threecrowd[:cassandra_stats][:keyspace]

service_config = node.threecrowd[:scrapi][:service]
logging_config = node.threecrowd[:scrapi][:logging_config]

config_dir = "/etc/#{service_name}"

runit_service "#{service_name}" do
  template_name "#{service_name}"
  options({
      :gc_log_enable => logging_config[:gc_log_enable],
      :gc_log_path => File.join(logging_config[:log_dir], "gc.log"),
      :config_dir => config_dir,
      :config_file_name => "#{service_name}.conf",
      :config_logging_file_name => "#{service_name}.log4j.conf",
  })
end

package 'scrapi' do
  version '4.0.0-20120926233331'
  action :install
  options "--allow-unauthenticated --force-yes"
  notifies :restart, resources(:service => [ "#{service_name}" ]), :delayed
end

group service_config[:groupname]

user service_config[:username] do
  comment "#{service_name} system user"
  group service_config[:groupname]
  system true
  shell "/bin/false"
end

directory config_dir do
  mode "0755"
  owner "root"
  group "root"
end

node.threecrowd[:scrapi][:listen_ip] = node[:ipaddress]

template config_dir + "/#{service_name}.conf" do
  source "#{service_name}.conf.erb"
  variables node.threecrowd.to_hash
  mode "0644"
  owner "root"
  group "root"
end  

directory logging_config[:log_dir] do
  owner "root"
  group "root"
  mode "0755"
end

log_path = node.threecrowd[:scrapi][:logging_config][:log_dir] 

directory log_path do
  owner "#{service_name}"
  group "#{service_name}"
  mode '0755'
end

template config_dir + "/#{service_name}.log4j.conf" do
  source "#{service_name}.log4j.conf.erb"
  owner 'root'
  group 'root'
  mode '0644'
  variables node.threecrowd[:scrapi][:logging_config].to_hash
end

nagios_nrpecheck "check_scrapi_running" do
  command "#{node['nagios']['plugin_dir']}/check_procs"
  critical_condition "1:1"
  parameters " -u scrapi -a /opt/scrapi/bin/scrapi.jar"
  action :add
end

nagios_nrpecheck "check_scrapi_port" do
  command "#{node['nagios']['plugin_dir']}/check_tcp"
  parameters " -H #{node[:ipaddress]} -p 9912 -m 1 -c 1 -t 1"
  action :add
end

nagios_nrpecheck "check_scrapi_jmx_port" do
  command "#{node['nagios']['plugin_dir']}/check_tcp"
  parameters " -H localhost -p 50003 -m 1 -c 1 -t 1"
  action :add
end
