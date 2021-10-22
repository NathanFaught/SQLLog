using System;
using System.IO;
using System.Net;
using System.Diagnostics;
using System.Collections;
using System.Data.SqlClient;
using Procurios.Public;
using System.Data;
using Microsoft.SqlServer.Types;
using System.Data.SqlTypes;

namespace SQLLog
{
    class Program
    {
        static void Main(string[] args)
        {
            //Ignore bad certificates - like the self signed one AGS generates itself.
            ServicePointManager.ServerCertificateValidationCallback += delegate { return true; };

            string home = Directory.GetCurrentDirectory();

            var MyIni = new IniFile();
            string slastrun = MyIni.Read("LastRun");
            string sthisruntemp = MyIni.Read("ThisRunTemp");
            string sprevendtime = MyIni.Read("PrevEndTime");

            // Check for the existing of both temp values and initalize if missing.
            if (sthisruntemp == "") {
                MyIni.Write("ThisRunTemp", "0");
                sthisruntemp = "0";
            }
            if (sprevendtime == "") 
            { 
                MyIni.Write("PrevEndTime", "0");
                sprevendtime = "0";
            }

            double dthisrun = (DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1))).TotalMilliseconds;
            dthisrun = Math.Truncate(dthisrun);

            // If sthisruntemp != 0, assume the previous execution failed.  Restarting app using the same dthisrun value as last time.
            // this is necessary to allow restarting without causing the import app to skip logs from previous exeuction to now.
            if (sthisruntemp != "0")
            { 
                // Overwriting dthisrun as the previous execution failed.
                dthisrun = Double.Parse(sthisruntemp); 
            }
            else 
            {
                // if sthisruntemp, then set to dthisrun.  Upon successful execution of the program, this INI value will be set back to 0.
                MyIni.Write("ThisRunTemp", dthisrun.ToString()); 
            }

            string sthisrun = dthisrun.ToString();
            sthisrun = sthisrun.Substring(0, sthisrun.Length);

            string ExeFriendlyName = System.AppDomain.CurrentDomain.FriendlyName;
            string[] ExeNameBits = ExeFriendlyName.Split('.');
            string ExeName = ExeNameBits[0];

            bool debug = false;
            bool proxy = false;
            bool cleanlogs = false;
            //bool incremental = false;

            string TLogsURL = "";
            string TTokenURL = "";
            string TUser = "";
            string TPassword = "";

            string Tdbuser = "";
            string Tdbpassword = "";
            string Tdbname = "";
            string Tdbserver = "";
            string Tdbschema = "dbo";
            string Tpagesize = "1000";

            string Tfilter = "";

            string Tsrid = "null";

            int c = args.GetUpperBound(0);

            // Loop through arguments
            for (int n = 0; n < c; n++)
            {
                string thisKey = args[n].ToLower();
                string thisVal = args[n + 1].TrimEnd().TrimStart();

                // eval the key
                switch (thisKey)
                {
                    case "-logsurl":
                        TLogsURL = thisVal;
                        break;                    
                    case "-debug":
                        string dbg = thisVal;
                        if (dbg.ToUpper() == "Y") debug = true;
                        break;
                    case "-proxy":
                        string prx = thisVal;
                        if (prx.ToUpper() == "Y") proxy = true;
                        break;
                    case "-cleanlogs":
                        string clg = thisVal;
                        if (clg.ToUpper() == "Y") cleanlogs = true;
                        break;
                    case "-user":
                        TUser = thisVal;
                        break;
                    case "-password":
                        TPassword = thisVal;
                        break;
                    case "-tokenurl":
                        TTokenURL = thisVal;
                        break;
                    case "-dbuser":
                        Tdbuser = thisVal;
                        break;
                    case "-dbpassword":
                        Tdbpassword = thisVal;
                        break;
                    case "-dbname":
                        Tdbname = thisVal;
                        break;
                    case "-dbserver":
                        Tdbserver = thisVal;
                        break;
                    case "-dbschema":
                        Tdbschema = thisVal;
                        break;
                    case "-filter":
                        Tfilter = thisVal;
                        break;
                    case "-srid":
                        Tsrid = thisVal;
                        break;
                    //case "-incremental":
                    //    string inc = thisVal;
                    //    if (inc.ToUpper() == "Y") incremental = true;
                    //    break;
                    case "-pagesize":
                        Tpagesize = thisVal;
                        break;
                    default:
                        break;
                }
            }

            if (debug == true) Console.WriteLine("Last run " + slastrun);
            if (debug == true) Console.WriteLine("This run " + sthisrun);

            if (TLogsURL == "") return;

            string Token = "";

            string logsurl = TLogsURL;

            

            WebClient client = new WebClient();

            if (proxy == true)
            {
                client.Proxy = WebRequest.DefaultWebProxy;
                client.Proxy.Credentials = CredentialCache.DefaultCredentials;
            }

            if (TTokenURL != "" && TPassword != "" && TUser != "")
            {
                if (TTokenURL.EndsWith("/") == false) TTokenURL = TTokenURL + "/";

                Token = GetToken2(TTokenURL, TUser, TPassword);
                if (Token.Contains("Token Error:"))
                {
                    Console.WriteLine(Token);
                    Environment.Exit(-1);
                }
            }

            string json = "";

            try
            {
                if (Token != "")
                {
                    json = client.DownloadString(new Uri(logsurl + "?f=json&token=" + Token));
                }
                else
                {
                    json = client.DownloadString(new Uri(logsurl + "?f=json"));
                }

            }
            catch (WebException webEx)
            {
                Console.WriteLine(webEx.Message);
                Environment.Exit(-1);
            }                

            //Some Esri error
            if (json.ToLower().Contains("error"))
            {
                Console.WriteLine(json);
                Environment.Exit(-1);
            }

            if (debug == true) Console.WriteLine(logsurl);
            if (debug == true) Console.WriteLine("");

            Stopwatch sw = Stopwatch.StartNew();

            bool hasMore = true;
            //double prevEndTime = 0;
            double prevEndTime = Double.Parse(sprevendtime);
            int requests = 0;

            while (hasMore == true)
            {
                string imgurl = logsurl + "/query?";

                if (debug == true) Console.WriteLine("Previous End Time = " + prevEndTime.ToString());

                //If there is no last run and this is the first request then use sinceLastStart 
                if (slastrun == "" && prevEndTime == 0)
                {
                    if (debug == true) Console.WriteLine("Using sinceLastStart=true (must be first run)");
                    imgurl = imgurl + "&sinceLastStart=true";
                }
                else
                {
                    //If we have a previous end time we must be in a hasMore loop so set the startTime
                    if (prevEndTime > 0)
                    {
                        if (debug == true) Console.WriteLine("prevEndTime > 0 (" + prevEndTime.ToString() + ")");
                        //startTime = most recent time to query
                        imgurl = imgurl + "&startTime=" + prevEndTime.ToString();
                    }

                    //If we have a last run then use that to limit the results
                    if (slastrun != "")
                    {
                        if (debug == true) Console.WriteLine("Using endTime=" + slastrun);
                        imgurl = imgurl + "&endTime=" + slastrun;
                    }
                    else
                    {
                        if (debug == true) Console.WriteLine("Using sinceLastStart=true (must be first run but prevEndTime > 0)");
                        imgurl = imgurl + "&sinceLastStart=true";
                    }
                }

                imgurl = imgurl + "&level=FINE";
                imgurl = imgurl + "&filterType=json";
                imgurl = imgurl + "&filter=" + Tfilter; //"{\"server\": \"*\", \"services\": \"*\", \"machines\":\"*\" }";

                imgurl = imgurl + "&pageSize=" + Tpagesize;
                imgurl = imgurl + "&f=json";                

                if (Token != "")
                {
                    //Check token is valid, if not get a new one.
                    if (CheckTokenValid(logsurl, Token, proxy, debug) == false)
                    {
                        if (debug == true) Console.WriteLine("Token expired, get a new one.");
                        if (debug == true) Console.WriteLine("Old token=" + Token);
                        Token = GetToken2(TTokenURL, TUser, TPassword);
                        if (debug == true) Console.WriteLine("New token=" + Token);
                    }

                    imgurl = imgurl + "&token=" + Token;
                }

                string response = "";

                try
                {
                    requests++;
                    response = client.DownloadString(new Uri(imgurl));
                    if (debug == true) Console.WriteLine(imgurl + " OK");
                    int result = ProcessResponse(response, Tdbuser, Tdbpassword, Tdbname, Tdbserver, Tdbschema, debug, Tsrid);

                    if (result < 0)
                    {
                        Console.WriteLine("Error inserting records");
                        System.Environment.Exit(1);
                    }

                    Console.WriteLine("Inserted " + result.ToString() + " records");

                    Hashtable root;
                    root = (Hashtable)Procurios.Public.JSON.JsonDecode(response);
                    hasMore = (bool)root["hasMore"];
                    if (hasMore == true) prevEndTime = (double)root["endTime"];

                    //PrevEndTime allows the program to restart where it left off in the event of program failure..
                    MyIni.Write("PrevEndTime", prevEndTime.ToString());

                }
                catch (WebException webEx)
                {
                    if (debug == true) Console.WriteLine(imgurl + " " + webEx.Message);
                    hasMore = false;
                }
            }

            sw.Stop();

            double seconds = sw.ElapsedMilliseconds;

            if (debug == true) Console.WriteLine("Made " + requests.ToString() + " successful requests in " + (seconds/1000).ToString() + " seconds");

            if (debug == true) Console.WriteLine("Cleaning out logs (" + cleanlogs.ToString() + ")");

            if (cleanlogs == true)
            {
                string cleanurl = logsurl + "/clean?";

                cleanurl = cleanurl + "f=json";

                if (Token != "") cleanurl = cleanurl + "&token=" + Token;

                try
                {
                    client.UploadString(new Uri(cleanurl), "");
                    if (debug == true) Console.WriteLine(cleanurl + " OK");
                }
                catch (WebException webEx)
                {
                    if (debug == true) Console.WriteLine(cleanurl + " " + webEx.Message);
                    System.Environment.Exit(1);
                }
            }

            /* 
             * After successful completion, update 
             * LastRun = program start DateTime 
             * PrevEndTime = 0
             * ThisRunTemp = 0
             * 
             * Note, if PrevEndTime has a value other than zero, it indicates the program is currently running or the program failed.
             * PrevEndTime allows the program to restart where it left off.
             */
            MyIni.Write("LastRun", sthisrun);
            MyIni.Write("ThisRunTemp", "0");
            MyIni.Write("PrevEndTime", "0");

            Console.WriteLine("Done!");
        }

        public static bool CheckTokenValid(string logsurl, string Token, bool proxy, bool debug)
        {
            WebClient client = new WebClient();

            if (proxy == true)
            {
                client.Proxy = WebRequest.DefaultWebProxy;
                client.Proxy.Credentials = CredentialCache.DefaultCredentials;
            }

            string json = "";

            try
            {
                json = client.DownloadString(new Uri(logsurl + "?f=json&token=" + Token));
            }
            catch (WebException webEx)
            {
                if (debug == true) Console.WriteLine(webEx.Message);
                return false;
            }

            //Token expired
            if (json.ToLower().Contains("token expired"))
            {
                if (debug == true) Console.WriteLine(json);
                return false;
            }

            //Some other Esri error
            if (json.ToLower().Contains("error"))
            {
                if (debug == true) Console.WriteLine(json);
                return false;
            }

            return true;
        }

        public static int ProcessResponse(string response, string dbuser, string dbpassword, string dbname, string dbserver, string dbschema, bool debug, string srid)
        {
            Hashtable root;
            ArrayList LogRecords;
            long LogRecordsCount = 0;

            root = (Hashtable)Procurios.Public.JSON.JsonDecode(response);

            LogRecords = (ArrayList)root["logMessages"];

            if (LogRecords == null)
            {
                if (debug == true) Console.WriteLine("LogRecords is null");
                return -1;
            }

            LogRecordsCount = LogRecords.Count;

            if (debug == true) Console.WriteLine("Retrieved " + LogRecordsCount.ToString() + " records");

            SqlConnection myConnection;

            if (dbuser != "")
            {
                myConnection = new SqlConnection("Server=" + dbserver + "; Database=" + dbname + "; User ID=" + dbuser + "; Password=" + dbpassword);
            }
            else
            {
                myConnection = new SqlConnection("Server=" + dbserver + "; Database=" + dbname + ";Integrated Security=true");
            }

            try
            {
                myConnection.Open();
            }
            catch (Exception e)
            {
                Console.WriteLine("Could not open up SQL Connection");
                if (debug == true) Console.WriteLine(e.ToString());
                return -1;
            }

            /*
             *Creating store all records captured by JSON response in a Table object.  
             *After the Table object is fully loaded, it will be bulk loaded into SQL
            */
            DataTable tbl = new DataTable();
            tbl.Columns.Add(new DataColumn("type", typeof(string)));
            tbl.Columns.Add(new DataColumn("message", typeof(string)));
            tbl.Columns.Add(new DataColumn("time", typeof(DateTime)));
            tbl.Columns.Add(new DataColumn("source", typeof(string)));
            tbl.Columns.Add(new DataColumn("machine", typeof(string)));
            tbl.Columns.Add(new DataColumn("username", typeof(string)));
            tbl.Columns.Add(new DataColumn("code", typeof(double)));
            tbl.Columns.Add(new DataColumn("elapsed", typeof(string)));
            tbl.Columns.Add(new DataColumn("process", typeof(string)));
            tbl.Columns.Add(new DataColumn("thread", typeof(string)));
            tbl.Columns.Add(new DataColumn("methodname", typeof(string)));
            tbl.Columns.Add(new DataColumn("mapsize_x", typeof(Int32)));
            tbl.Columns.Add(new DataColumn("mapsize_y", typeof(Int32)));
            tbl.Columns.Add(new DataColumn("mapscale", typeof(Double)));
            tbl.Columns.Add(new DataColumn("mapextent_minx", typeof(Double)));
            tbl.Columns.Add(new DataColumn("mapextent_miny", typeof(Double)));
            tbl.Columns.Add(new DataColumn("mapextent_maxx", typeof(Double)));
            tbl.Columns.Add(new DataColumn("mapextent_maxy", typeof(Double)));
            tbl.Columns.Add(new DataColumn("Shape", typeof(SqlGeometry)));

            int n;

            for (n = 0; n < LogRecordsCount; n++)
            {
                Hashtable LogRecord = (Hashtable)LogRecords[n];
                System.Data.DataRow dr = tbl.NewRow();

                string type = (string)LogRecord["type"];
                string message = (string)LogRecord["message"];
                double dtime = (double)LogRecord["time"];
                string source = (string)LogRecord["source"];
                string machine = (string)LogRecord["machine"];
                string user = (string)LogRecord["user"];
                double code = (double)LogRecord["code"];
                string elapsed = (string)LogRecord["elapsed"];
                string process = (string)LogRecord["process"];
                string thread = (string)LogRecord["thread"];
                string methodName = (string)LogRecord["methodName"];

                string scale = "NULL";

                string size_x = "NULL";
                string size_y = "NULL";

                string minx = "NULL";
                string miny = "NULL";
                string maxx = "NULL";
                string maxy = "NULL";

                string polygon;

                DateTime dttime = UnixTimeStampToDateTime2(dtime);
                dttime = (dttime.ToLocalTime());

                // Data must be truncated to the maximum length of the SQL columns before SQLBulkCopy executes
                if (type.Length > 50) type = type.Substring(0, 50);
                if (message.Length > 4000) message = message.Substring(0, 4000);
                if (source.Length > 100) source = source.Substring(0, 100);
                if (machine.Length > 50) machine = machine.Substring(0, 50);
                if (user.Length > 50) user = user.Substring(0, 50);
                if (elapsed.Length > 50) elapsed = elapsed.Substring(0, 50);
                if (process.Length > 50) process = process.Substring(0, 50);
                if (thread.Length > 50) thread = thread.Substring(0, 50);
                if (methodName.Length > 50) methodName = methodName.Substring(0, 50);

                message = message.Replace("'", "''");

                dr["type"] = type;
                dr["message"] = message;
                dr["time"] = dttime;
                dr["source"] = source;
                dr["machine"] = machine;
                dr["username"] = user;
                dr["code"] = code;
                dr["elapsed"] = elapsed;
                dr["process"] = process;
                dr["thread"] = thread;
                dr["methodname"] = methodName;

                if (message.Contains("Extent:"))
                {
                    string[] vals = message.Split(';');

                    string tmp_extent_all = vals[0];
                    string[] tmp_extent = vals[0].Split(':');
                    string[] tmp_size = vals[1].Split(':');
                    string[] tmp_scale = vals[2].Split(':');

                    string[] tmp_sizes = tmp_size[1].Split(',');
                    string[] tmp_extents = tmp_extent[1].Split(',');

                    scale = tmp_scale[1];

                    size_x = tmp_sizes[0];
                    size_y = tmp_sizes[1];

                    dr["mapsize_x"] = Int32.Parse(size_x);
                    dr["mapsize_y"] = Int32.Parse(size_y);
                    dr["mapscale"] = Double.Parse(scale);

                    if (tmp_extent_all.ToUpper().Contains("NAN") == false)
                    {
                        minx = tmp_extents[0];
                        miny = tmp_extents[1];
                        maxx = tmp_extents[2];
                        maxy = tmp_extents[3];

                        dr["mapextent_minx"] = Double.Parse(minx);
                        dr["mapextent_miny"] = Double.Parse(miny);
                        dr["mapextent_maxx"] = Double.Parse(maxx);
                        dr["mapextent_maxy"] = Double.Parse(maxy);

                        // Commented out Shape column to save space.

                        polygon = "POLYGON((" + minx + " " + miny + "," + minx + " " + maxy + "," + maxx + " " + maxy + "," + maxx + " " + miny + "," + minx + " " + miny + "))";
                        dr["Shape"] = SqlGeometry.STPolyFromText(new SqlChars(new SqlString(polygon)), Int32.Parse(srid));
                    }
                    else
                    {
                        dr["mapextent_minx"] = DBNull.Value;
                        dr["mapextent_miny"] = DBNull.Value;
                        dr["mapextent_maxx"] = DBNull.Value;
                        dr["mapextent_maxy"] = DBNull.Value;
                        dr["Shape"] = DBNull.Value;
                    }
                }
                else
                {
                    dr["mapsize_x"] = DBNull.Value;
                    dr["mapsize_y"] = DBNull.Value;
                    dr["mapscale"] = DBNull.Value;
                    dr["mapextent_minx"] = DBNull.Value;
                    dr["mapextent_miny"] = DBNull.Value;
                    dr["mapextent_maxx"] = DBNull.Value;
                    dr["mapextent_maxy"] = DBNull.Value;
                    dr["Shape"] = DBNull.Value;
                }

                tbl.Rows.Add(dr);

            }
            //Finished adding rows the DataTable, attempting to bulk insert now.
            //Performing bulk insert to save time as inserting 1 record takes as much SQL time as inserting 10,000 records
            //Clock testing of original version (1 insert per loop) - 35 seconds for 2000 records
            //Clock testing of updated  version (pagesize inserts per loop) - 12 seconds for 10000 records
            try
            {
                SqlBulkCopy objbulk = new SqlBulkCopy(myConnection);
                objbulk.DestinationTableName = dbschema + ".RawLogs";

                objbulk.ColumnMappings.Add("type", "type");
                objbulk.ColumnMappings.Add("message", "message");
                objbulk.ColumnMappings.Add("time", "time");
                objbulk.ColumnMappings.Add("source", "source");
                objbulk.ColumnMappings.Add("machine", "machine");
                objbulk.ColumnMappings.Add("username", "username");
                objbulk.ColumnMappings.Add("code", "code");
                objbulk.ColumnMappings.Add("elapsed", "elapsed");
                objbulk.ColumnMappings.Add("process", "process");
                objbulk.ColumnMappings.Add("thread", "thread");
                objbulk.ColumnMappings.Add("methodName", "methodname");
                objbulk.ColumnMappings.Add("mapsize_x", "mapsize_x");
                objbulk.ColumnMappings.Add("mapsize_y", "mapsize_y");
                objbulk.ColumnMappings.Add("mapscale", "mapscale");
                objbulk.ColumnMappings.Add("mapextent_minx", "mapextent_minx");
                objbulk.ColumnMappings.Add("mapextent_miny", "mapextent_miny");
                objbulk.ColumnMappings.Add("mapextent_maxx", "mapextent_maxx");
                objbulk.ColumnMappings.Add("mapextent_maxy", "mapextent_maxy");
                objbulk.ColumnMappings.Add("Shape", "Shape");

                objbulk.WriteToServer(tbl);

                myConnection.Close();
                tbl.Clear();

            }
            catch (Exception e)
            {
                Console.WriteLine("Could not execute SQL statement");
                Console.WriteLine(e.ToString());
                myConnection.Close();
                tbl.Clear();
                return -1;
            }

            if (debug == true) Console.WriteLine("Inserted records " + LogRecordsCount.ToString());
            return n;
        }

        public static DateTime UnixTimeStampToDateTime(double unixTimeStamp)
        {
            // Unix timestamp is seconds past epoch
            System.DateTime dtDateTime = new DateTime(1970, 1, 1, 0, 0, 0, 0);
            dtDateTime = dtDateTime.AddSeconds(unixTimeStamp).ToLocalTime();
            return dtDateTime;
        }

        public static double DateTimeToUnixTimestamp(DateTime dateTime)
        {
            return (dateTime - new DateTime(1970, 1, 1).ToLocalTime()).TotalSeconds;
        }

        static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
        static readonly double MaxUnixSeconds = (DateTime.MaxValue - UnixEpoch).TotalSeconds;

        public static DateTime UnixTimeStampToDateTime2(double unixTimeStamp)
        {
            return unixTimeStamp > MaxUnixSeconds
               ? UnixEpoch.AddMilliseconds(unixTimeStamp)
               : UnixEpoch.AddSeconds(unixTimeStamp);
        }

        public static string GetToken(string tokenurl, string username, string password)
        {
            string url = tokenurl + "?request=getToken&username=" + username + "&password=" + password + "&expiration=60";

            System.Net.WebRequest request = System.Net.WebRequest.Create(url);

            string myToken = "";

            try
            {
                System.Net.WebResponse response = request.GetResponse();
                System.IO.Stream responseStream = response.GetResponseStream();
                System.IO.StreamReader readStream = new System.IO.StreamReader(responseStream);

                myToken = readStream.ReadToEnd();
            }

            catch (WebException we)
            {
                myToken = "Token Error: " + we.Message;
            }

            return myToken;
        }

        public static string GetToken2(string tokenurl, string username, string password)
        {
            string url = tokenurl;
            string param = "request=getToken&f=json&username=" + username + "&password=" + password + "&expiration=60&client=requestip";

            System.Net.WebRequest request = System.Net.WebRequest.Create(url);
            request.Credentials = System.Net.CredentialCache.DefaultNetworkCredentials;

            request.Method = "POST";
            request.ContentType = "application/x-www-form-urlencoded";
            request.ContentLength = param.Length;

            StreamWriter requestWriter = new StreamWriter(request.GetRequestStream());
            requestWriter.Write(param);
            requestWriter.Close();

            string myToken = "";
            string myJSON = "";

            try
            {
                System.Net.WebResponse response = request.GetResponse();
                System.IO.Stream responseStream = response.GetResponseStream();
                System.IO.StreamReader readStream = new System.IO.StreamReader(responseStream);

                myJSON = readStream.ReadToEnd();

                Hashtable LogRecord = (Hashtable)JSON.JsonDecode(myJSON);

                myToken = (string)LogRecord["token"];
            }

            catch (WebException we)
            {
                myToken = "Token Error: " + we.Message;
            }

            return myToken;
        }
    }
}
