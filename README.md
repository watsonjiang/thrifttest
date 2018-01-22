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

 
   
