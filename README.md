# EntityFramework.BulkExtensions
Bulk operations extension for Entity Framework.

This project was built as an extension to add bulk operations functionality to the Entity Framework. 
It works as extension methods of the DBContext class. It supports transaction if the context's database have a CurrentTransaction, or it creates an internal one for the scope of the operation.

Exemple usage:

var entityList = new IList<MyEntity>();<br>
entityList.Add(new Myentity());<br>
entityList.Add(new Myentity());<br>
entityList.Add(new Myentity());<br>

//Bulk insert extension method<br>
context.BulkInsert(entityList); 
