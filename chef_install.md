## Chef脚本管理工具部署

## 部署节点
节点类型 | IP |
-----| ------ |
Server | 192.168.10.191 |
Workstation | 192.168.10.36 |
Node | 192.168.10.35 |

## 安装的版本
* Chef-Server：chef-server-11.1.0-1.el6.x86_64.rpm
* Chef-Client：chef-11.10.0-1.el6.x86_64.rpm

## 安装前需要了解的
首先Chef的官网有许多的发布版本，首先按照操作系统类别来分主要有Red Hat和Ubantu两种版本，根据目前我们所用的机子，选择Red Hat版本的，
版本不要选的太高，可能因为库版本不一致导致安装失败，而且错选安装系统类型也会导致某些文件不一致导致安装的失败。

## 一.Chef Serve的安装
注:Chef Server需要安装在物理机上，在虚拟机节点上安装可能会导致rabbbq启动不起来，或者安装过程中出现阻塞现象，具体原因是虚拟机上的某些
文件会覆盖真实系统的文件导致跑不起来。
### Chef Server安装前需要做的
* 配置主机名，因为后面的访很多都是以域名的方式访问
  ```
  vim /etc/sysconfig/network 
  HOSTNAME=chef.server.com
  ```
  
* 更改/etc/hosts文件，添加一条记录
  ```
  vim /etc/hosts
  192.168.10.191 chef.server.com
  ```
  
* 如果线上有设置防火墙的话，开启443端口，因为Chef Server 11.xx版本都是监听443端口的
  ```
  iptables -I INPUT -p tcp --dport 443 -j ACCEPT
  service iptables save
  ```
  
### 安装步骤
* 下载rpm安装包，例如chef-server-11.1.0-1.el6.x86_64.rpm,下载的方式可以从官网中下载，也可以用下面命令行的方式
  ```
  wget -c --no-check-certificate (rpm包url地址)
  ```
 
* 终端内进入chef-server软件包所在目录，执行以下命令
  ```
  rpm -ivh chef-server-11.1.0-1.el6.x86_64.rpm
  ```
* 执行成功后，执行下面的配置命令
  ```
  chef-server-ctl reconfigure
  ```
  
* 执行成功后，浏览器内输入(https://192.168.10.191)，或者域名方式访问(https://chef.server.com)，会看到一个登陆界面，表明Chef Server已经安装成功
用户名密码如，即可登系统(注意此时先不要急着改密码)
  ```
  username: admin
  password: p@ssw0rd1
  ```

在此过程中，你可能会出现类似安装进程卡住，等待错误提示，那可能是因为你的系统版本选择不对，这个对server端的影响挺大的
## 二.Workstation的安装
### 安装前需要做的
* 同样需要修改hostname和/etc/hosts文件，写入hosts文件时还要chef server的ip一起写入操作如下
  ```
  vim /etc/sysconfig/network 
  HOSTNAME=chef.workstation.com
  
  vim /etc/hosts
  192.168.10.36 chef.workstation.com
  #写入chef server ip
  192.168.10.191 chef.server.com
  ```

