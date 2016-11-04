# EntityFramework.BulkExtensions
Bulk operations extension for Entity Framework.

This project was built as an extension to add bulk operations functionality to the Entity Framework. 
It works as extension methods of the DBContext class. It uses the context transaction or an internal one if there is no
current transaction on the context.

Exemple usage:

var entityList = new IList<MyEntity>();<br>
entityList.Add(new Myentity());<br>
entityList.Add(new Myentity());<br>
entityList.Add(new Myentity());<br>

//Bulk insert extension method<br>
context.BulkInsert(entityList); 
