# This may be better named "DeadLockSafePool" as the standard connections _are_ thread safe, they just don't handle
# concurrency in a safe manner.  You could end up with a nasty dead lock if you reserve a job then call reserve
# on that same connection before deleting the job.  That's what led me tob build this...
#
# A given job may only be acted upon by one client (i.e. connection) during a given off-stalk cycle.  That is,
# if a job is reserved by connection A, it must be deleted, released or kicked by connection A.  
#
# In a multithreaded environment one often wishes to have the main event loop continuously call .reserve and
# pass each job off to a worker thread.  That job-processing thread would then handle the work and call job.delete
# upon completion (or some other appropriate method depending on the outcome of the work).  This job-processing
# thread needs to use the original connection for the .delete call.  Beanstalk::ThreadedPool maintains unavailable 
# connections - those with outstanding jobs - and does not make them available for use.
#
# A call to .reserve_connection will return a free connection, waiting for one to become available if necessary.
# A call to .try_reserve_connection will return a free connection or nil if there are no free ones.
#
# If you have 1 main loop calling reserve and N job-processing threads, you should create a ThreadedPool with a 
# size of N+1.
#
# *****************************
# CAVEAT: If you are using a connection c from the ThreadedPool to do anything other than .reserve and then work on a job,
# you currently need to call c.return_to_pool after performing your operation.  Otherwise, the ThreadedPool will not
# automatically return the connection it the available pool of connections like it does on delete, realease, and bury.
# *****************************
#
# A contrived code example?  Sure...
# Given: Some producer is putting jobs on a tube named "tubetop" and you want to use a new thread to process each job.
#
# conn_pool = Beanstalk::ThreadedPool.new('localhost:11300', 10, "tubetop")
# while true
#   conn = conn_pool.reserve_connection
#   job = c.reserve()
#   Thread.new(j) { |j| do_work_on(j); j.delete; }
# end
#
module Beanstalk
  class ThreadedConnection < Connection
    
    def initialize(addr, threaded_pool, default_tube=nil)
      super(addr, default_tube)
      @threaded_pool = threaded_pool
    end

    #On delete, release, bury: need to tell the pool that we're usable again, as we're no longer responsible for a job
    def delete(id)
      interact("delete #{id}\r\n", %w(DELETED))
      return_to_pool
      :ok
    end

    def release(id, pri, delay)
      id = id.to_i
      pri = pri.to_i
      delay = delay.to_i
      interact("release #{id} #{pri} #{delay}\r\n", %w(RELEASED))
      return_to_pool
      :ok
    end
    
    def bury(id, pri)
      interact("bury #{id} #{pri}\r\n", %w(BURIED))
      return_to_pool
      :ok
    end
    
    def return_to_pool() @threaded_pool.make_connection_available(self); end
    
  end
    
  class ThreadedPool < Pool
  
    def initialize(addr, pool_size=1, default_tube=nil)
      @addr = addr
      @pool_mutex = Mutex.new
      @pool_size = pool_size
      @watch_list = ['default']
      @default_tube=default_tube
      @watch_list = [default_tube] if default_tube
      connect()
    end
  
    def connect()
      @pool_mutex.lock
      
      @connections ||= []
      @unavailable_connections ||= []
      @available_connections ||= []
    
      (@pool_size - @connections.size).times do
        begin
          conn = ThreadedConnection.new(@addr, self, @default_tube)
          prev_watched = conn.list_tubes_watched()
          to_ignore = prev_watched - @watch_list
          @watch_list.each{|tube| conn.watch(tube)}
          to_ignore.each{|tube| conn.ignore(tube)}
          @connections << conn
          @available_connections << conn
        rescue Errno::ECONNREFUSED
          raise NotConnected
        rescue Exception => ex
          puts "#{ex.class}: #{ex}"
        end
      end
    
    ensure
      @pool_mutex.unlock
      @connections.size
    end
    
    def make_connection_available(conn)
      @pool_mutex.lock
      @unavailable_connections.delete(conn)
      @available_connections << conn
    ensure
      @pool_mutex.unlock
    end
    
    # Returns the first available connection, if there is one
    # If there are none available, returns nil.
    def try_reserve_connection
      connect()
      @pool_mutex.lock
      conn = @available_connections.first
      if conn
        @available_connections.delete(conn)
        @unavailable_connections << conn
      end
    ensure
      @pool_mutex.unlock
      return conn
    end
    
    # Returns an available connection, yielding control and spinning until one can be had
    def reserve_connection
      conn = try_reserve_connection
      if conn
        return conn
      else
        #Are there no full semaphores?  Just these binary ones (aka mutexes)?
        Thread.pass
        sleep(0.5)
        return reserve_connection
      end
    end
  
  end
  
end