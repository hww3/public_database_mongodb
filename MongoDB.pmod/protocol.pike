constant OP_REPLY = 1;
constant OP_MSG = 1000;
constant OP_UPDATE = 2001;
constant OP_INSERT = 2002;
constant OP_QUERY = 2004;
constant OP_GET_MORE = 2005;
constant OP_DELETE = 2006;
constant OP_KILL_CURSORS = 2007;

constant FLAG_UPSERT = 0b1;
constant FLAG_MULTIUPDATE = 0b10;

constant FLAG_RESERVED = 0;
constant FLAG_TAILABLE_CURSOR = 1;
constant FLAG_SLAVE_OK = 2;
constant FLAG_OPLOG_REPLAY = 3;
constant FLAG_NO_CURSOR_TIMEOUT = 4;
constant FLAG_AWAIT_DATA = 5;
constant FLAG_EXHAUST = 6;
constant FLAG_PARTIAL = 7;

constant FLAG_SINGLE_REMOVE = 0b1;

constant FLAG_CURSOR_NOT_FOUND = 0b1;
constant FLAG_QUERY_FAILURE = 0b10;
constant FLAG_SHARD_CONFIG_STATE = 0b100;
constant FLAG_AWAIT_CAPABLE = 0b1000;

string buffer = "";

string toBSON(mixed native)
{
	return "";
}

string toCString(string str)
{
	if(search(str, "\0") != -1) throw(Error.Generic("String cannot contain null bytes.\n"));
	else return string_to_utf8(str) + "\0";
}

string gen_header(int type, int responseTo, int requestId, string mesg)
{
	return sprintf("%-4c%-4c%-4c%-4c", 16+sizeof(mesg), requestId, responseTo, type);
}

string gen_op_update(int requestId, string collectionName, int flags, mapping selector, mapping update)
{
	string up = sprintf("%-4c%s%-4c%s%s", 0, toCString(collectionName), flags, BSON.toDocument(selector), BSON.toDocument(update));
	return gen_header(OP_UPDATE, 0, requestId|| random(99999999), up) + up;
}

string gen_op_insert(int requestId, string collectionName, array|mapping documents)
{
	if(mappingp(documents))
	  documents = ({ documents });
	string up = sprintf("%-4c%s%s", 0, toCString(collectionName), BSON.toDocumentArray(documents));
	return gen_header(OP_INSERT, 0, requestId|| random(99999999), up) + up;
}

string gen_op_query(int requestId, string collectionName, int flags, mapping query, int numbertoSkip, int numberToReturn, mapping|void returnFields)
{
	string up = sprintf("%-4c%s%-4c%-4c%s%s", flags, toCString(collectionName), numbertoSkip, numberToReturn, BSON.toDocument(query), (returnFields?BSON.toDocument(returnFields):""));
	return gen_header(OP_QUERY, 0, requestId|| random(99999999), up) + up;	
}

string gen_op_get_more(int requestId, string collectionName, int numberToReturn, int cursorId)
{
	string up = sprintf("%-4c%s%-4c%-8c", 0, toCString(collectionName), numberToReturn, cursorId);
	return gen_header(OP_GET_MORE, 0, requestId|| random(99999999), up) + up;	
}

string gen_op_delete(int requestId, string collectionName, int flags, mixed selector)
{
	string up = sprintf("%-4c%s%-4c%s", 0, toCString(collectionName), flags, toBSON(selector));
	return gen_header(OP_DELETE, 0, requestId|| random(99999999), up) + up;
}

string gen_op_kill_cursors(int requestId, array(int) cursorsToKill)
{
	string up = sprintf("%-4c%-4c%{%-8c}", 0, sizeof(cursorsToKill), cursorsToKill);
	return gen_header(OP_KILL_CURSORS, 0, requestId|| random(99999999), up) + up;	
}

mapping parse_reply()
{
	int len;
	int requestId;
	int responseTo;
	int opcode;

    int responseFlags;
    int cursorId;
    int startingFrom;
    int numberReturned;

    string data;

    array returnedDocuments;
	
	if(!sscanf(buffer, "%-4c", len))
		throw(Error.Generic("not enough data to parse.\n"));
//	werror("response len: %O buffer size: %O\n", len, sizeof(buffer));
	if(sizeof(buffer)>= (len)) // we can parse it now
	{
	  sscanf(buffer, "%*-4c%" + (len-4) + "s%s", data, buffer);
	  if(sscanf(data, "%-4c%-4c%-4c%s", requestId, responseTo, opcode, data)!=4)
		throw(Error.Generic("could not parse message header.\n"));
//werror("reply: %O\n", data);	
	  if(opcode == OP_REPLY)
	  {
		if(sscanf(data, "%-4c%-8c%-4c%-4c%s", responseFlags, cursorId, startingFrom, numberReturned, data)!=5)
			throw(Error.Generic("unable to parse reply message.\n"));
//	    werror("number returned: %O, data: %O\n", numberReturned, data);
		if(numberReturned)
		  returnedDocuments = BSON.fromDocumentArray(data);
	  }
	  else
		throw(Error.Generic("unexpected message type " + opcode + ".\n"));
	}
	else
	{
		return 0;
	}
	
	mapping reply = ([]);

	reply->requestId = requestId;
	reply->responseTo = responseTo;
	reply->responseFlags = responseFlags;
	reply->cursorId = cursorId;
	reply->numberReturned = numberReturned;
	reply->startingFrom = startingFrom;
	reply->returnedDocuments = returnedDocuments;

    return reply;
}

