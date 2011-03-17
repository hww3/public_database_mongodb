//! A module for working with the BSON format.
//!

private int counter;

constant TYPE_FLOAT = 0x01;
constant TYPE_STRING = 0x02;
constant TYPE_DOCUMENT = 0x03;
constant TYPE_ARRAY = 0x04;
constant TYPE_BINARY = 0x05;
constant TYPE_OBJECTID = 0x07;
constant TYPE_BOOLEAN = 0x08;
constant TYPE_DATETIME = 0x09;
constant TYPE_NULL = 0x0a;
constant TYPE_REGEX = 0x0b;
constant TYPE_INT32 = 0x10;
constant TYPE_INT64 = 0x12;


/* TODO: still need to support the following types:

	| 	"\x05" e_name binary 	Binary data
	| 	"\x0B" e_name cstring cstring 	Regular expression
	| 	"\x0D" e_name string 	JavaScript code
	| 	"\x0E" e_name string 	Symbol
	| 	"\x0F" e_name code_w_s 	JavaScript code w/ scope
	| 	"\x11" e_name int64 	Timestamp
	| 	"\xFF" e_name 	Min key
	| 	"\x7F" e_name 	Max key	
*/

int getCounter()
{
  return ++counter;
}

//!
string toDocument(mapping m)
{
  String.Buffer buf = String.Buffer();
  encode(m, buf);
  return sprintf("%-4c%s%c", sizeof(buf)+5, buf->get(), 0);
}


static void encode(mixed m, String.Buffer buf)
{
  foreach(m; mixed key; mixed val)
  {
    if(!stringp(key)) throw(Error.Generic("BSON Keys must be strings.\n"));
    if(search(key, "\0") != -1) throw(Error.Generic("BSON Keys may not contain NULL characters.\n"));   
    
    key = string_to_utf8(key);
    encode_value(key, val, buf);
  }	
}

static void encode_value(string key, mixed value, String.Buffer buf)
{
   if(floatp(value))
   { 
     buf->add(sprintf("%c%s%c%8F", TYPE_FLOAT, key, 0, value));
   }
   else if(stringp(value))
   {
     string v = string_to_utf8(value);
     buf->add(sprintf("%c%s%c%-4c%s%c", TYPE_STRING, key, 0, sizeof(v)+1, v, 0));
   }
   else if(mappingp(value))
   {
     buf->add(sprintf("%c%s%c%s", TYPE_DOCUMENT, key, 0, toDocument(value)));
   }
   else if(arrayp(value))
   {
     int qi = 0; 
     buf->add(sprintf("%c%s%c%s", TYPE_ARRAY, key, 0, toDocument(mkmapping(map(value, lambda(mixed e){return (string)qi++;}), value))));
   }
   else if(intp(value))
   {
   	   // werror("have int\n");
     // 32 bit or 64 bit?
     if(value <= 2147383647 && value >= -2148483648) // we fit in a 32 bit space.
     {
     	     // werror("32bit\n");
       buf->add(sprintf("%c%s%c%-4c", TYPE_INT32, key, 0, value));       
     }
     else
     {
     	     // werror("64bit\n");
       buf->add(sprintf("%c%s%c%-8c", TYPE_INT64, key, 0, value));       
     }
   }
   else if(objectp(value) && value->unix_time && value->utc_offset) // a date object
   {
     buf->add(sprintf("%c%s%c%-8c", TYPE_DATETIME, key, 0, (value->unix_time() /* + value->utc_offset() */ * 1000)));
   }
   else if(objectp(value) && Program.implements(object_program(value), BSON.ObjectId))
   {
     buf->add(sprintf("%c%s%c%12s", TYPE_OBJECTID, key, 0, value->get_id()));
   }
   else if(objectp(value) && value == Null)
   {
     buf->add(sprintf("%c%s%c", TYPE_NULL, key, 0));
   }
   else if(objectp(value) && value == True)
   {
     buf->add(sprintf("%c%s%c%c", TYPE_BOOLEAN, key, 0, 1));
   }
   else if(objectp(value) && value == False)
   {
     buf->add(sprintf("%c%s%c%c", TYPE_BOOLEAN, key, 0, 0));
   }
   
   //werror("bufsize: %O\n", sizeof(buf));
}

//!
mixed fromDocument(string bson)
{
  int len;
  string slist;
  if(sscanf(bson, "%-4c%s", len, bson)!=2)
    throw(Error.Generic("Unable to read length from BSON stream.\n"));
  if(sscanf(bson, "%" + (len-5) + "s\0", slist) != 1)
    throw(Error.Generic("Unable to read full data from BSON stream.\n"));
//werror("bson length %d\n", len);
  mapping list = ([]);
  
  do
  {
    slist = decode_next_value(slist, list);
  } while(sizeof(slist));
  
  return list;	
}

string toDocumentArray(array documents)
{
	String.Buffer buf = String.Buffer();
	
	foreach(documents;;mixed document)
	{
		buf->add(toDocument(document));
	}
	
	return buf->get();
}

array fromDocumentArray(string bsonarray)
{
  array a = ({});

  while(sizeof(bsonarray))
  {
	string bson;
	int len;
	
 	if(sscanf(bsonarray, "%-4c", len)!=1)
	  throw(Error.Generic("Unable to read length from BSON stream.\n"));
 	if(sscanf(bsonarray, "%" + len + "s%s", bson, bsonarray) != 2)
	  throw(Error.Generic("Unable to read full data from BSON stream.\n"));
//	werror("parsing BSON: %O\n", bson);
	a+=({fromDocument(bson)});
//	werror("bsonarray: %O\n", bsonarray);
  }
//  werror("done parsing.\n");
  return a;
}

static string decode_next_value(string slist, mapping list)
{
  string key;
  string values;
  mixed value;

  int type;
  
  string document;
  int doclen;
  
  if(sscanf(slist, "%c%s\0%s", type, key, slist)!=3)
    throw(Error.Generic("Unable to read key and type from BSON stream.\n")); 

  key = utf8_to_string(key);

  switch(type)
  {
     case TYPE_FLOAT:
       if(sscanf(slist, "%8F%s", value, slist) != 2)
         throw(Error.Generic("Unable to read float from BSON stream.\n")); 
       break;
     case TYPE_STRING:
       int len;
       if(sscanf(slist, "%-4c%s", len, slist) != 2)
         throw(Error.Generic("Unable to read string length from BSON stream.\n")); 
 	if(sscanf(slist, "%" + (len-1) + "s\0%s", value, slist) != 2)
         throw(Error.Generic("Unable to read string from BSON stream.\n")); 
 	value = utf8_to_string(value);
       break;
     case TYPE_INT32:     
       if(sscanf(slist, "%-4c%s", value, slist) != 2)
         throw(Error.Generic("Unable to read int32 from BSON stream.\n")); 
       break;
     case TYPE_INT64:
       if(sscanf(slist, "%-8c%s", value, slist) != 2)
         throw(Error.Generic("Unable to read int64 from BSON stream.\n")); 
       break;
     case TYPE_OBJECTID:
       if(sscanf(slist, "%12s%s", value, slist) != 2)
         throw(Error.Generic("Unable to read object id from BSON stream.\n")); 
 	value = .ObjectId(value);
       break;
     case TYPE_BOOLEAN:
       if(sscanf(slist, "%c%s", value, slist) != 2)
         throw(Error.Generic("Unable to read boolean from BSON stream.\n")); 
       if(value) value = True;
       else value = False;
       break;
     case TYPE_NULL:
       value = Null;
       break;
     case TYPE_DATETIME:
       if(sscanf(slist, "%-8c%s", value, slist) != 2)
         throw(Error.Generic("Unable to read datetime from BSON stream.\n"));      
         value/=1000;
         value = Calendar.Second("unix", value);
       break;
     case TYPE_DOCUMENT:
       if(sscanf(slist, "%-4c", doclen) != 1)
       	 throw(Error.Generic("Unable to read embedded document length\n"));
       if(sscanf(slist, "%" + (doclen) + "s%s", document, slist) !=2)
       	 throw(Error.Generic("Unable to read specified length for embedded document.\n"));
       value = fromDocument(document);
       break;
     case TYPE_ARRAY:
       if(sscanf(slist, "%-4c", doclen) != 1)
       	 throw(Error.Generic("Unable to read embedded document length\n"));
       if(sscanf(slist, "%" + (doclen) + "s%s", document, slist) !=2)
       	 throw(Error.Generic("Unable to read specified length for embedded document.\n"));
       value = fromDocument(document);
       int asize = sizeof(value);
       array bval = allocate(asize);
       for(int i = 0; i < asize; i++)
       {
       	 bval[i] = value[(string)i];
       }
       value=bval;
//       value=predef::values(value);
       break;
     default:
       throw(Error.Generic("Unknown BSON type " + type + ".\n"));     
  }
  
  list[key] = value;
  // werror("type: %d key %s\n", type, key);
  return slist;
}

//!
object Null = null();

//!
object True = true_object();

//!
object False = false_object();

class false_object
{
  constant BSONFalse = 1;

  static mixed cast(string type)
  {
    if(type == "string")
      return "false";
    if(type == "int")
      return 0;
  }
}

class true_object
{
  constant BSONTrue = 1;

  static mixed cast(string type)
  {
    if(type == "string")
      return "false";
    if(type == "int")
      return 1;
  }
}

class null
{
  constant BSONNull = 1;

  static string cast(string type)
  {
    if(type == "string")
      return "null";
  }
}

