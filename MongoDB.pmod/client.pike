inherit .protocol;

Stdio.File conn;

static void create(string host, int port)
{
	conn = Stdio.File();
    if(!conn->connect(host, port))
      throw(Error.Generic("Unable to connect to " + host + ":" + port + ".\n"));
//	write("res: %O\n", conn->connect(host, port));
}

void send_command(string cmd)
{
	conn->write(cmd);
}

mapping read_reply()
{
  mapping reply;

  do
  {
  	string s = conn->read(1024, 1);

  	if(s) buffer += s;

//write("buf: %O\n", buffer);
  	reply = parse_reply();
 
    if(reply)
      return reply;

  } while(!reply);

  return reply;
}

mapping send_query(string collectionName, int flags, mapping query, int start, int quantity, mapping|void returnFields)
{
   send_command(gen_op_query(0, collectionName, flags, query, start, quantity, returnFields));
   return read_reply();
}


void send_insert(string collectionName, array|mapping documents)
{
   send_command(gen_op_insert(0, collectionName, documents));
}