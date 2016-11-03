using System.Collections.Generic;
using System.Data.Entity;
using System.Data.SqlClient;
using System.Threading.Tasks;
using EntityFramework.BulkExtensions.Helpers;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    internal interface ITransaction
    {
        int CommitTransaction<T>(DbContext context, IEnumerable<T> collection, ColumnDirection columnDirection) where T : class;
        Task<int> CommitTransactionAsync<T>(DbContext context, IEnumerable<T> collection);
    }
}
