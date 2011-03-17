require '../lib/beanstalk-client.rb'

t_start = Time.now
how_many = 100
pool_size = 20
threads = []
do_extra_reserve = false

#----------------------boot up
puts "booting..."
conns = Beanstalk::ThreadedPool.new('localhost:11300', pool_size, "tubedan")


#----------------------add jobs
puts "putting #{how_many}"
conn = conns.reserve_connection
how_many.times do |i|
  job = { :payload => "job #{i} payload @ #{Time.now}" }
  conn.put(job)
end
conn.return_to_pool


#----------------------get those fucking jobs!
puts "getting #{how_many} via #{how_many} threads, #{pool_size} connection in thread pool"
how_many.times do
  c = conns.reserve_connection
  j = c.reserve()
  threads << Thread.new(j) do |j|
    #sleep(0.1 * rand(20))
    puts "trying to delete job #{j.id}..."
    puts "got job #{j.id} deleting... #{j.delete}"
  end
end


if do_extra_reserve
  puts "++++      reserving one extra time       ++++"
  no_j = conns.reserve_connection.reserve()
  puts "*****************************************************GOT NO J, DIDNT EXPECT THAT"
end


#----------------------wait for threads to complete work
threads.each { |t| t.join }


#---------------------- stats...
t_end = Time.now
puts "It took #{(t_end - t_start)} seconds to process #{how_many} jobs with #{pool_size} threaded connections in pool"

puts "-------------stats---------------"
puts conns.reserve_connection.stats_tube "tubedan"

puts "all done"