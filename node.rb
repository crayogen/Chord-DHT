require 'socket'
require 'digest/sha1'
require 'json'
require 'fileutils'

$bits = 12
size = 2**$bits

$mutex = Mutex.new

###########################################################################################
# CHORD CLASS CHORD CLASS CHORD CLASS CHORD CLASS CHORD CLASS CHORD CLASS CHORD CLASS CHO #
###########################################################################################

class Node
  attr_reader :fingerTable, :node
  attr_accessor :predecessor, :successors

  def initialize host, port, folder
    node_id = Digest::SHA1.hexdigest (port + host)
    @bits, @size = $bits, 2**$bits
    @id = node_id.to_i(16) % @size
    puts "id", @id
    # puts "your id is: " + node_id.to_s
    @node = {"id"=> @id, "port"=> port}
    Dir.chdir("files/" + folder) do
      @path = Dir.pwd
    end
    @successors = [{"id"=> @id, "port"=> port}]
    @predecessor = nil
    initFingerTable
  end

  def initFingerTable
    table_size = @bits
    id, port = @node["id"], @node["port"]
    @fingerTable = Array.new(table_size)  { |i| { "id_begin"=> (@id+2**i) % @size,
                                                  "id_end"=> (@id+2**(i+1)) % @size,
                                                  "succ_id"=> @id,
                                                  "port"=> port
                                              } 
                                        }
  end

  def socketCommunicate (to_send, port)
    begin
      s = TCPSocket.open('localhost', port)
      s.puts (to_send.to_json)
      JSON.parse s.gets
    rescue
      puts to_send, port
      puts "RESUCE!!!!!!!!"
    end
  end

  def inRange?(start, finish, n)
    return n>=start && n<finish if start < finish
    finish = finish + @size
    return n<finish if n>=start
    n = n + @size
    return n>=start && n<finish
  end

  def isAlive port
    return true if port == @node["port"]
    begin
      alive = true
      3.times {
        s = TCPSocket.open('localhost', port)
        s.puts ({"type"=>"ping"}).to_json
        reply = JSON.parse (s.gets)
        alive = reply["type"] == "pong" ? alive : false
      }
      return alive
    rescue
      false
    end
  end

  def fixSuccessors
    succ = @successors[0]
    port = succ["port"]
    return true if port == @node["port"]
    unless isAlive port
      @successors.delete_at(0)
      fixSuccessors
    else
      true
    end
  end

  def getSuccessor
    fixSuccessors
    @successors[0]
  end

  def allFiles
    files = Dir[@path+"/*"]
    files.select { |f| f[-4..-1] != "temp" }
  end

  def sendFilesToSuccessors successors
    return if @predecessor == nil
    files = allFiles
    files = files.select { |f| ( inRange? @predecessor["id"], @node["id"], (fileHash f) ) }
    successors.each do |succ|
      if isAlive succ["port"]
        sendFiles files, succ["port"], true
      end
    end
  end

  def createSuccessorTable
    succ = getSuccessor
    to_send = {"type"=> "getSuccessorsTable"}
    succTable = socketCommunicate(to_send, succ["port"])
    len = succTable.length >= 1 ? 1 : succTable.length
    @successors[1, len] = succTable[0, len]
    sendFilesToSuccessors @successors
  end

  def findSuccessor id
    pred = findPredecessor id
    return @node if pred == nil
    to_send = {"type"=> "getSuccessor"}
    succ_node = pred["port"]==@node["port"] ? getSuccessor : socketCommunicate(to_send, pred["port"])
    succ_node
  end

  def findPredecessor id
    succ = getSuccessor
    succ_id = succ["id"]
    my_id = @node["id"]
    if my_id == id
      pred = @predecessor
    elsif (inRange?(my_id+1, succ_id, id) || succ_id == id)
      pred = @node
    else
      jump_to = closestPredeceedingFinger id
      port = jump_to["port"]
      port = succ["port"] if port == @node["port"]
      to_send = {"type"=> "findPredecessor", "id"=> id}
      pred = socketCommunicate(to_send, port)
    end
    return pred
  end

  def closestPredeceedingFinger id
    my_id = @node["id"]
    pred = @fingerTable.reverse.detect{ |f| ( inRange?(my_id, id, f["succ_id"]) && (isAlive f["port"]) )}
    pred || @node
  end

  def join port
    to_send = {"type"=> "findSuccessor", "id"=> @node["id"]}
    @successors[0] = socketCommunicate(to_send, port)
    files = allFiles
    files.each do |file|
      file_id = fileHash file
      succ = findSuccessor file_id
      sendAFile file, succ["port"], false
    end
  end

  def sendAFile file, port, copy
    return true if port == @node["port"]
    to_send = {"type"=>"sendingFile", "file"=>file}
    if File.exist? file
      $mutex.synchronize do
        s = TCPSocket.open('localhost', port)
        s.puts (to_send.to_json)
        File.open(file, 'rb') do |f|
          while chunk = f.read(1024)
            begin
              s.write chunk
            rescue
              break
            end
          end
          s.close
        end
        File.delete file unless copy
      end
    else
      false
    end
  end

  def requestFile? file_name, port
    file_directory = @path + "/" + file_name
    files = allFiles
    requested_file = files.detect { |f| f==file_directory }
    if requested_file
      puts "requested", requested_file
      sendAFile requested_file, port, true if requested_file
    end
    return requested_file
  end

  def putFile file_dir
    file_id = fileHash file_dir
    puts file_dir, file_id
    # file_name = ( file_dir.match (/([^\/]+)$/) ) .to_s
    # new_path = @path + "/" + file_name
    succ = findSuccessor file_id
    if File.exist? file_dir
      if succ["port"] == @node["port"]
        FileUtils.cp file_dir, @path 
        return
      end
      sendAFile file_dir, succ["port"], true
    end
  end

  def getFile file_name
    puts "in getFile function", file_name
    file_id = fileHash file_name
    puts file_id
    file_dest = @path+"/"+file_name
    puts file_dest
    unless File.exist? file_dest
      succ = findSuccessor file_id
      puts succ
      to_send = { "type"=> "requestFile?", "file_name"=>file_name, "port"=>@node["port"]}
      reply = socketCommunicate to_send, succ["port"]
      return reply["file_exists"]
    end
    true
  end

  def sendFiles files, port, copy
    files.each { |file| sendAFile file, port, copy }
  end

  def leave
    succ = getSuccessor
    succ_port = succ["port"]
    return if succ_port == @node["port"]
    files = allFiles
    sendFiles files, succ_port, false
    puts "ok to leave.."
  end

  def stabalize
    succ = getSuccessor
    if succ["port"] == @node["port"]
      succ_pred = @predecessor
    else
      to_send = {"type"=> "getPredecessor"}
      succ_pred = socketCommunicate(to_send, succ["port"])
    end
    my_id = @node["id"]
    if succ_pred == nil
    elsif inRange? my_id+1, succ["id"], succ_pred["id"]
      if isAlive succ_pred["port"]
        succ["id"], succ["port"] = succ_pred["id"], succ_pred["port"]
      end
    end
    unless succ["port"] == @node["port"]
      to_send = {"type"=> "notify", "node"=>@node}
      socketCommunicate(to_send, succ["port"])
      createSuccessorTable
    end
    files = allFiles
    print "my id "
    puts @node["id"]
    files.each { |f| puts f, (fileHash f)}
  end

  def notify node
    to_notify = false
    if ( @predecessor == nil )
      to_notify = true
    else
      in_range = inRange? @predecessor["id"]+1, @node["id"], node["id"]
      is_dead = true unless isAlive @predecessor["port"]
      to_notify = true if (in_range || is_dead)
    end
    if to_notify
      files = allFiles
      files = files.select { |f| ( inRange? @node["id"], node["id"], (fileHash f) ) }
      @predecessor = node
      sendFiles files, @predecessor["port"], false
    end
  end

  def fixFinger finger
    succ_node = findSuccessor finger["id_begin"]
    finger["succ_id"], finger["port"] = succ_node["id"], succ_node["port"]
  end

  def fixRandomFinger
    i = rand(@bits-1)
    finger_to_update = @fingerTable[i]
    fixFinger finger_to_update
    puts @fingerTable, " "
    puts "succ", @successors, " "
  end

  def getHash string
    node_id = Digest::SHA1.hexdigest string
    node_id.to_i(16) % @size
  end

  def fileHash file_directory
    file_name = file_directory.match (/([^\/]+)$/)
    getHash file_name.to_s
  end

  def testing
    STDOUT.flush
    # puts "pred", @predecessor
    # puts @successors
    # puts ""
  end
end

###########################################################################################
# CHORD CLASS CHORD CLASS CHORD CLASS CHORD CLASS CHORD CLASS CHORD CLASS CHORD CLASS CHO #
###########################################################################################


###########################################################################################
# GUI GUI GUI GUI GUI GUI GUI GUI GUI GUI GUI GUI GUI GUI GUI GUI GUI GUI GUI GUI GUI GUI #
###########################################################################################

require 'gtk3'
include Gtk

class ChordApp < Gtk::Window
  def initialize
    @host, @port, @port_to_contact = "127.0.0.1", "", ""
    super
    init_ui
  end

  def putButtonPress file_path
    begin
      if File.exist? file_path
        $node.putFile file_path
      else
        on_file_erro
      end
    rescue
      on_file_erro
    end
  end
  
  def getButtonPress file_name
    begin

      found = $node.getFile file_name
      on_file_erro unless found
    rescue
      on_file_erro
    end
  end

  def on_file_erro
    md = Gtk::MessageDialog.new :parent => self, 
      :flags => :modal, :type => :error, 
      :buttons_type => :close, :message => "file or path does not exist"
    md.run
    md.destroy
  end

  def on_invalid_ip_erro
    md = Gtk::MessageDialog.new :parent => self, 
      :flags => :modal, :type => :error, 
      :buttons_type => :close, :message => "invalid port or host"
    md.run
    md.destroy
  end

  def init_ui
    set_title  "Chord DHT"
    set_border_width 10
    override_background_color :normal, Gdk::RGBA::new(1.0, 1.0, 1.0, 1.0)

    signal_connect "destroy" do
        Gtk.main_quit
    end

    fixed = Gtk::Fixed.new

    # white = Gdk::RGBA::new(1.0, 1.0, 1.0, 1.0)
    # first_node_cb.override_background_color(:normal, white)

    get_file_entry = Gtk::Entry.new
    put_file_entry = Gtk::Entry.new
    host_entry = Gtk::Entry.new
    host_entry.set_text "127.0.0.1"
    port_entry = Gtk::Entry.new
    port_to_contact_entry = Gtk::Entry.new


    put_file_entry.signal_connect "key-release-event" do |w, e|
        @put_file = w.text
    end

    get_file_entry.signal_connect "key-release-event" do |w, e|
        @get_file = w.text
    end

    host_entry.signal_connect "key-release-event" do |w, e|
        @host = w.text
    end

    port_entry.signal_connect "key-release-event" do |w, e|
        @port = w.text
    end

    port_to_contact_entry.signal_connect "key-release-event" do |w, e|
        @port_to_contact = w.text
    end
    
    get_file_button = Gtk::Button.new :label =>'get file'
    get_file_button.sensitive = false
    get_file_button.set_tooltip_text "write the file name\nremember to add file extention (e.g .txt)"
    get_file_button.signal_connect "clicked" do
      puts "asking around for ", @get_file
      getButtonPress @get_file
      get_file_entry.set_text ""
    end

    put_file_button = Gtk::Button.new :label =>'put file'
    put_file_button.sensitive = false
    put_file_button.set_tooltip_text "add the file path"
    put_file_button.signal_connect "clicked" do
      putButtonPress @put_file
      put_file_entry.set_text ""
    end

    valid_port = /^([0-9]{1,4}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$/
    port_to_contact_button = Gtk::Button.new :label => 'contact port'
    port_to_contact_button.sensitive = false
    port_to_contact_button.signal_connect "clicked" do
      if @port_to_contact.match(valid_port)
        @first_node = false
        start_listening
        put_file_button.sensitive = true
        get_file_button.sensitive = true
        port_to_contact_button.sensitive = false
      else
        on_invalid_ip_erro
      end
    end

    first_node_cb = Gtk::CheckButton.new "First node"
    first_node_cb.sensitive = false
    first_node_cb.signal_connect("clicked") do |w|
      put_file_button.sensitive = true
      get_file_button.sensitive = true
      @first_node = true
      port_to_contact_entry.set_text ""
      start_listening
      port_to_contact_button.sensitive = false
    end

    port_button = Gtk::Button.new :label => 'port'
    port_button.sensitive = false
    port_button.signal_connect "clicked" do
      if @port.match(valid_port)
        port_to_contact_button.sensitive = true
        first_node_cb.sensitive = true
        port_button.sensitive = false
      else
        on_invalid_ip_erro
      end
      # port_entry.set_text ""
    end

    host_button = Gtk::Button.new :label => 'host'
    host_button.signal_connect "clicked" do
      valid_host = /^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$/
      if @host.match(valid_host)
        host_button.sensitive = false
        port_button.sensitive = true
        # host_entry.set_text ""
      else
        on_invalid_ip_erro
      end
    end

    quit_button = Gtk::Button.new :label => "Quit"
    quit_button.sensitive = false
    quit_button.signal_connect "clicked" do
      Gtk.main_quit
    end


    fixed.put host_entry, 20, 30
    fixed.put host_button, 150, 30
    fixed.put port_entry, 250, 30
    fixed.put port_button, 380, 30
    fixed.put port_to_contact_entry, 140, 80
    fixed.put port_to_contact_button, 320, 80
    fixed.put first_node_cb, 150, 120 
    fixed.put put_file_entry, 50, 300
    fixed.put put_file_button, 240, 300
    fixed.put get_file_entry, 50, 340  
    fixed.put get_file_button, 240, 340   

    fixed.put quit_button, 700, 400

    add fixed
    # on_init

    set_tooltip_text "Chord App"
    set_default_size 600, 350
    set_window_position :center
    
    show_all
  end

  def start_listening
    port_to_contact = @port_to_contact
    port = @port
    first_node = @first_node
    host = @host

    folder = ARGV[0]
    $node = Node.new(host, port, folder)
    start host, port, port_to_contact, first_node
  end

end

###########################################################################################
# GUI GUI GUI GUI GUI GUI GUI GUI GUI GUI GUI GUI GUI GUI GUI GUI GUI GUI GUI GUI GUI GUI #
###########################################################################################

def start host, port, port_to_contact, first_node
  folder = ARGV[0]
  # puts "starting", host, port, port_to_contact, first_node
  Thread.new {
    unless first_node
      puts "not the first so joining using socket to connect"
      s = TCPSocket.open(host, port)
      $node.join port_to_contact
    end
    while true
      sleep(2)
      $node.stabalize
      $node.fixRandomFinger
      $node.testing
    end
  }

  Thread.new {
    node_server = TCPServer.open(host, port)
    loop {
      Thread.start(node_server.accept) do |node_client|
        data = node_client.gets
        msg = {"type" => nil}
        msg = JSON.parse(data) if data

        if msg["type"] == "ping"
          node_client.puts ({"type"=>"pong"}).to_json
        end
        if msg["type"] == "getNode"
          node_client.puts ($node.node).to_json
        end
        if msg["type"] == "getPredecessor"
          node_client.puts ($node.predecessor).to_json
        end
        if msg["type"] == "getSuccessorsTable"
          node_client.puts ($node.successors).to_json
        end
        if msg["type"] == "findPredecessor"
          pred_node = $node.findPredecessor msg["id"]
          node_client.puts pred_node.to_json
        end
        if msg["type"] == "getSuccessor"
          succ_node = $node.getSuccessor
          node_client.puts succ_node.to_json
        end
        if msg["type"] == "findSuccessor"
          succ_node = $node.findSuccessor msg["id"]
          node_client.puts succ_node.to_json
        end
        if msg["type"] == "notify"
          $node.notify msg["node"]
          reply = { "type"=> "done" }.to_json
          node_client.puts reply
        end
        if msg["type"] == "getPredecessor"
          node_client.puts ($node.predecessor).to_json
        end
        if msg["type"] == "requestFile?"
          file_name = msg["file_name"]
          port = msg["port"]
          reply = {"file_exists"=>nil}
          reply["file_exists"] = ($node.requestFile? file_name, port) ? true : false
          node_client.puts reply.to_json
        end
        if msg["type"] == "sendingFile"
          file = msg["file"] 
          f_name = "files/" + folder + "/" + (file.match (/([^\/]+)$/)).to_s
          temp_file = f_name+".temp"
          unless File.exist? f_name or File.exist? temp_file
            File.open(temp_file, 'wb') do |file|
              # begin
              while chunk = node_client.read(1024)
                break if chunk.empty?
                file.write chunk
              end
              # rescue
              #   f = (file.match (/([^\/]+)$/)).to_s
              # end
            end
            # puts "now changin name"
            $mutex.synchronize do
              File.rename temp_file, f_name
            end
          end
        end
        node_client.close
      end
    }
  }
end

window = ChordApp.new
Gtk.main

# Thread.new {
  
  # puts "Hi There!"
  # while true
  #   puts "Enter 1 to leave."
  #   puts "Enter 2 to search and download for file."
  #   puts "Enter 3 to add a file to the DHT."
  #   input = STDIN.gets.chomp
  #   if input == "1"
  #     node.leave
  #     exit(0)
  #   elsif input == "2"
  #     puts "Enter file name. Remember to add the file extention (e.g .txt)"
  #     f_name = STDIN.gets.chomp
  #     found = $node.getFile f_name
  #     puts found ? "file exists, downloading.." :  "file not found!"
  #   elsif input == "3"
  #     puts "Enter file directory. Remember to add the file extention (e.g .txt)"
  #     f_dir = STDIN.gets.chomp
  #     $node.putFile f_dir
  #     puts "adding file to DHT"
  #   end
  # end
  # puts "left"
# }

