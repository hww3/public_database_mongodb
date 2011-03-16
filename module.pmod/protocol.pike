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
	return sprintf("%4c%4c%4c%4c", 16+sizeof(mesg), requestId, responseTo, type);
}

string gen_op_update(int requestId, string collectionName, int flags, mapping selector, mapping update)
{
	string up = sprintf("%4c%s%4c%s%s", 0, toCString(collectionName), flags, BSON.toDocument(selector), map(update, BSON.toDocument)*"");
	return gen_header(OP_UPDATE, 0, requestId|| random(99999999), up) + up;
}

string gen_op_insert(int requestId, string collectionName, array documents)
{
	string up = sprintf("%4c%s%s", 0, toCString(collectionName), BSON.toDocument(documents));
	return gen_header(OP_INSERT, 0, requestId|| random(99999999), up) + up;
}

string gen_op_query(int requestId, string collectionName, int flags, mapping query, int start, int quantity, mapping|void returnFields)
{
	string up = sprintf("%4c%s%4c%4c%s%s", flags, toCString(collectionName), numbertoSkip, numberToReturn, BSON.toDocument(query), (returnFields?BSON.toDocument(returnFields):""));
	return gen_header(OP_QUERY, 0, requestId|| random(99999999), up) + up;	
}

string gen_op_get_more(int requestId, string collectionName, int numberToReturn, int cursorId)
{
	string up = sprintf("%4c%s%4c%8c", 0, toCString(collectionName), numbertoReturn, numberToReturn, cursorId);
	return gen_header(OP_GET_MORE, 0, requestId|| random(99999999), up) + up;	
}

string gen_op_delete(int requestId, string collectionName, int flags, mixed selector)
{
	string up = sprintf("%4c%s%4c%s", 0, toCString(collectionName), flags, toBSON(selector));
	return gen_header(OP_DELETE, 0, requestId|| random(99999999), up) + up;
}

string gen_op_kill_cursors(int requestId, array(int) cursorsToKill)
{
	string up = sprintf("%4c%4c%{%8c}", 0, sizeof(cursorsToKill), cursorsToKill);
	return gen_header(OP_KILL_CURSORS, 0, requestId|| random(99999999), up) + up;	
}

