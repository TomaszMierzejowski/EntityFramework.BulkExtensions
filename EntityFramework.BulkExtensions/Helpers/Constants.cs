namespace EntityFramework.BulkExtensions.Helpers
{
    internal static class Constants
    {
        internal const string InternalId = "BulkExtensions_InternalId";
        internal const string TempOutputTableName = "#TmpOutput";
        internal const string SourceAlias = "Source";
        internal const string TargetAlias = "Target";
        internal const string UniqueParamIdentifier = "Condition";

    }

#pragma warning disable 1591
    public enum ColumnDirection
    {        
        Input, InputOutput       
    }
    

    internal enum OperationType
    {
        Insert, InsertOrUpdate, Update, Delete
    }
#pragma warning restore 1591
}
