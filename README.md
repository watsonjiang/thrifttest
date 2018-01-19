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
    arbiter生成worker进程时
   
   
