Beanstalk Ruby Client
=====================

Beanstalk is a simple, fast work queue. Its interface is generic, but was
originally designed for reducing the latency of page views in high-volume web
applications by running time-consuming tasks asynchronously.

For more information, see:

 - <http://kr.github.com/beanstalkd/>
 - <http://github.com/kr/beanstalkd/raw/master/doc/protocol.txt>

## Notes

This library has been synchronized internally to handle usage from multiple
concurrent threads.

*BUT* if you wish to use it in a concurrent environment you should see threaded_connection.
The ThreadedPool handles pooling and reserving of connections so as to prevent dead lock conditions.

## Contributors

 - Dan Spinosa
 - Isaac Feliu
 - Peter Kieltyka
 - Martyn Loughran
 - Dustin Sallings
