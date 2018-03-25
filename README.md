a python thrift rpc test.

features:
   1 connection pooling
   2 reconnect
   3 service discovery
   4 auto failover
   5 load balance


参考gunicorn，引入多进程模式，进程内使用gevent协程的方式避开python的GIL。

worker 每个子进程作为一个worker，所有worker监听父进程打开的侦听句柄，接受连接并处理

arbiter 负责建立侦听句柄并管理子进程.

迟加载：
    参考gunicorn， 提供命令thriftsvr [options] <app>
    其中options是参数，app是应用（多个应用逗号分隔)
    thriftsvr启动arbiter，派生子进程。
    子进程派生完成后再加载应用
    arbiter进程不加载应用任何信息. 为了防止子进程import扰乱arbiter加载器

worker防呆：
    由于worker本质上是单线程程序，处理不当容易卡死。需要引入防呆机制。
    arbiter生成worker进程时生成一个临时文件。
    worker进程定期touch临时文件。
    arbiter定期检查临时文件时间戳，若超过规定时间没有更新文件，判定worker已经卡死。
    arbiter发送信号杀掉卡死worker

动态升级(TODO)：
    业务逻辑经常发生变更，对于thrift这类使用长连接的rpc，更新业务逻辑代码需要重启进程，中断连接。客户端能感知到连接异常。
    是否可以做业务端无感知的升级？
    解耦arbiter进程与业务进程。arbiter进程不加载业务代码。业务代码在worker进程加载。
    升级时，新增加的worker进程加载新代码，老的worker进程使用旧代码。
    老worker进程结束时，将掌控的连接句柄优雅的传给新worker进程。句柄请求将由新逻辑处理。
    所有老worker进程结束，升级完成。
    升级过程客户端完全无感知。
     
无损增减worker数量(TODO):
    减少worker数量时，被杀的进程所有的连接会关闭，客户端能感知到连接异常。
    如果关闭worker前能把worker所有的连接转给其它worker，则客户端不会感知到worker减少事件。
    增加worker数量时，由于thrift基于长连接，没有新连接的场景，新加的worker不能分流负载。
    如果增加worker后，其它worker能把手里的连接转给新加的worker，就能立刻发挥增加worker的作用。

 
服务发现：
    使用zk作服务注册
    每个servie接口在zk里暴露一个临时节点
    example:
/service/com.watson.orion/00000001

{
    ‘service’: ‘com.watson.orion’
    ‘version’: ‘0.0.1’
    ‘label’: [‘test’, ‘slow’]
    ‘ip’:’127.0.0.1’
    ‘port’: 12345
    ‘extra’:{}
}

节点信息是immutable的
一旦创建，不能改变。
改变需要取消原节点，增加新节点

     服务端约定:
       1 服务初始化完成后注册
       2 网络原因造成zk session失效后, 服务端重新注册一个新节点
     客户端约定：
       1 监听服务节点下的子节点变更，发现新增服务节点
       2 监听服务节点下的子节点变更，删除消失的服务节点

python thriftsvr/app/luncher.py -a 127.0.0.1:9999 -w 10 -v -n test server:app1