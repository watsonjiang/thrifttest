namespace py watson

exception LogException {
   1:i32 code;
   2:string reason;
}

service logging
{
    void log(1:string message);
    
    void log1(1:string message) throws(1:LogException ex);
}
