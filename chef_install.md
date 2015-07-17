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
**注:Chef Server需要安装在物理机上，在虚拟机节点上安装可能会导致rabbbq启动不起来，或者安装过程中出现阻塞现象，具体原因是虚拟机上的某些文件会覆盖真实系统的文件导致跑不起来。**

### (1).Chef Server安装前需要做的
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
  
### (2).具体安装步骤
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

### (3).Chef Server安装结果验证
执行成功后，浏览器内输入(https://192.168.10.191)，或者域名方式访问(https://chef.server.com)，会看到一个登陆界面，表明Chef Server已经安装成功
用户名密码如，即可登系统(注意此时先不要急着改密码)
  ```
  username: admin
  password: p@ssw0rd1
  ```

在此过程中，你可能会出现类似安装进程卡住，等待错误提示，那可能是因为你的系统版本选择不对，这个对server端的影响挺大的
## 二.Workstation的安装
### (1).安装前需要做的
* 同样需要修改hostname和/etc/hosts文件，写入hosts文件时还要chef server的ip一起写入操作如下
  ```
  vim /etc/sysconfig/network 
  HOSTNAME=chef.workstation.com
  
  vim /etc/hosts
  192.168.10.36 chef.workstation.com
  #写入chef server ip
  192.168.10.191 chef.server.com
  ```

### (2).具体安装步骤
* chef client的安装，下载好chef client rpm包，执行下面指令：
  ```
  rpm -ivh chef-11.10.0-1.el6.x86_64.rpm
  ```
  
* git的安装，执行下述命令：
  ```
  yum install git
  ```
* chef repo的git相应目录的创建，进入root主目录，git克隆chef repository，命令如下
  ```
  cd ~
  git clone git://github.com/opscode/chef-repo.git
  ```
  
* 如果worksation节点与che server节点是同在一个节点上的，无须拷贝admin.pem和chef-validator.pem私钥文件，如果不是同一节点则需要拷，拷贝过程如下(考虑到集群规模的扩大，建议server节点与workstations节点分开部署)：
  * 在chef-workstation节点上先创建/root/.chef目录,并将chef服务器上的/etc/chef-server/admin.pem和/etc/chef-server/chef-validator.pem文件拷贝到此目录。
  ```
  mkdir ~/.chef
  scp chef.example.com:/etc/chef-server/admin.pem ~/.chef
  scp chef.example.com:/etc/chef-server/chef-validator.pem ~/.chef
  ``` 
  
**注意：如在此过程前已经修改了server端的admin密码，则原有的/etc/chef-server/admin.pem将会失效，此时应到web ui页面上拷贝此时的admin的private key,在user栏目下点击generate private key,然后手动复制。**

* 执行knife configure -i命令进行初始化
  ```
  knife configure -i
  ```
  knife configure配置过程需要更改的配置:
    * server URL修改为chef服务器的地址https://chef.server.com:443, 
    * admin's private key路径改为/root/.chef/admin.pem
    * validation key路径改为/root/.chef/chef-validation.pem
    * chef repository地址输入/root/chef-repo
    * 按照提示创建一个用户和密码
  其余项保持默认值.
  
* 配置ruby路径，chef默认集成了一个ruby的稳定版本,需修改PATH变量，保证chef集成的ruby被优先使用.
  ```
  vim ~/.bash_profile
' export PATH="/opt/chef/embedded/bin:$PATH"'
  source ~/.bash_profile
  ```
  
### (3).Workstation安装结果验证
执行knife client list命令，返回client列表则配置成功.
  ```
  knife client list
  ------------------------
  chef-validator
  chef-webui 
  ```
  在此过程中最易出错的还是admin.pem和chef-validator.pem中途拷贝验证的出错，注意拷贝前密码是否被修改。
  
## 三.Chef Node的安装
与Chef Server和Workstation只需一次安装不同的是，Chef Node的安装就是普通的客户端节点的安装，诸如以后的节添加等都是此类的方式，操作相对来讲简单了许多。

### (1).具体安装步骤
* 修改hostname和/etc/hosts文件，写入hosts文件时还要将chef server和workstation的ip一起写入操作如下
  ```
  vim /etc/sysconfig/network 
  HOSTNAME=chef.node.com
  
  192.168.10.35 chef.node.com
  #写入chef server 和workstation ip
  vim /etc/hosts
  192.168.10.36 chef.workstation.com
  192.168.10.191 chef.server.com
  ```
  
* 安装chef-Client,下载好chef client rpm包，执行下面指令：
  ```
  rpm -ivh chef-11.10.0-1.el6.x86_64.rpm
  ```
  
* 在chef-worksation节点下，也就是192.168.10.36执行下面命令，workstation添加并远程配置当前节点：
  ```
  #中间的域名也可以是ip,root和123456是是待添加node节点的ssh登录账号和密码
  knife bootstrap chef.node.com -x root -P 123456
  ```
  bootstrap操作就是将此节点注册到server的操作
  
**注：此时容易如果出现Connection Refused的错误，查看node机器ssh默认的端口号22是否被改写，如若改写进行添加端口操作，命令如下**
  ```
  vim /etc/ssh/sshd_config
  #添加下面这行
  Port 22
  /etc/init.d/sshd restart
  ```
	
### (2).Chef Node安装结果验证
  在workstation节点下执行下述命令
  ```
  knife node list
  ———————-———————-———————
  chef.node.com
  ———————-———————-———————
  ```
  表明节点添加成功，还可以在Chef Server上进行查看，在node区域会看到che.node.com的节点存在。
