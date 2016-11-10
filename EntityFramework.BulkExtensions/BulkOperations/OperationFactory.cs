namespace EntityFramework.BulkExtensions.BulkOperations
{
    internal static class OperationFactory
    {
        internal static IBulkOperation BulkInsert
        {
            get
            {
                return new BulkInsert();
            }
        }

        internal static IBulkOperation BulkUpdate
        {
            get
            {
                return new BulkUpdate();
            }
        }
    }
}