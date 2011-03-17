require '../lib/beanstalk-client.rb'
require 'curb'

def do_curb_stuff(conn_r, conn_d, curl)
  j = conn_r.reserve()
  del_job = Proc.new do
    puts "trying to delete job #{j.id}..."
    puts "curl returned for job #{j.id} ==> deleting... #{conn_d.delete(j.id)}"
  end
  
  #CURB
  c = Curl::Easy.new("http://google.com") do |curl|
      curl.follow_location = true
      curl.on_success {|easy| del_job.call }
    end
  curl.add(c)
end





how_many = 20
threaded = true
threads = []
do_curl = true

puts "booting..."
bs_conn_1 = Beanstalk::Connection.new('localhost:11300', "tubedan") #Beanstalk::Pool.new(['localhost:11300'], "tubedan")
bs_conn_2 = Beanstalk::Connection.new('localhost:11300', "tubedan") #Beanstalk::Pool.new(['localhost:11300'], "tubedan")
curl_multi = Curl::Multi.new

puts "putting #{how_many}"
how_many.times do |i|
  job = { :payload => "job #{i} payload @ #{Time.now}" }
  bs_conn_1.put(job)
end


if threaded
  if do_curl
    puts "hitting curb #{how_many} times without threads"
    (how_many/4).times { do_curb_stuff(bs_conn_1, bs_conn_1, curl_multi) }
    Thread.new { curl_multi.perform }
    (how_many/4).times { do_curb_stuff(bs_conn_1, bs_conn_1, curl_multi) }
    Thread.new { curl_multi.perform }
    (how_many/4).times { do_curb_stuff(bs_conn_1, bs_conn_1, curl_multi) }
    Thread.new { curl_multi.perform }
    (how_many/4).times { do_curb_stuff(bs_conn_1, bs_conn_1, curl_multi) }
    Thread.new { curl_multi.perform }
    
    #but we can't exit just yet
    #normally, we'd loop waiting on reserve
    #puts "reserving one extra time..."
    #no_j = bs_conn_2.reserve()
    #puts "*****************************************************GOT NO J, DIDNT EXPECT THAT"
    sleep(10)
    
  else
    puts "getting #{how_many} via #{how_many} threads"
    how_many.times do
      j = bs_conn_1.reserve()
      threads << Thread.new(j) do |j|
        sleep(0.1 * rand(20))
        puts "trying to delete job #{j.id}..."
        puts "got job #{j.id} deleting... #{j.delete}"
      end
    end
    
    #puts "reserving one extra time..."
    #no_j = bs_conn_2.reserve()
    #puts "*****************************************************GOT NO J, DIDNT EXPECT THAT"
  
    threads.each { |t| t.join }
  end
  
else
  puts "getting #{how_many} serially"
  how_many.times do
    j = bs_conn_1.reserve()
    puts "got job #{j} deleting..."
    bs_conn_1.delete(j.id)
    puts "deleted"
  end
end

puts "-------------stats---------------"
puts bs_conn_1.stats_tube "tubedan"

puts "all done"
