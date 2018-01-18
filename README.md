## NotWebMatrix.Data

[![Build Status][win-build-badge]][win-builds]
[![Build Status][nix-build-badge]][nix-builds]
[![NuGet][nuget-badge]][nuget-pkg]
[![MyGet][myget-badge]][edge-pkgs]

This project is an Open Source clone of the [WebMatrix.Data][ms-wmd] library
that was published by Microsoft&reg; as part of [ASP.NET Web
Pages][aspnet-wp] and the [ASP.NET Web Matrix][aspnet-wm] product. While it
was originally designed to be 100% compatible, the API may evolve and deviate
over time to support more features while maintaining the simplicity and
ease-of-use that was the spirit of the original.

It targets [.NET Standard][netstd] 2.0.


## Reference

The following reference was derived from [the original
documentation][orig-ref-doc].

### `Database.Execute`

    Database.Execute(SQLstatement [,parameters])

Executes `SQLstatement` (with optional parameters) such as `INSERT`,
`DELETE`, or `UPDATE` and returns a count of affected records.

```c#
db.Execute("INSERT INTO Data (Name) VALUES ('Smith')");
db.Execute("INSERT INTO Data (Name) VALUES (@0)", "Smith");
```

### `Database.GetLastInsertId`

    Database.GetLastInsertId()

Returns the identity column from the most recently inserted row.

```c#
db.Execute("INSERT INTO Data (Name) VALUES ('Smith')");
var id = db.GetLastInsertId();
```

### `Database.Open`

    Database.Open(filename)
    Database.Open(connectionStringName)

Opens either the specified database file or the database specified using a
named connection string from the Web.config file.

```c#
// Note that no filename extension is specified.
var db = Database.Open("SmallBakery"); // Opens SmallBakery.sdf in App_Data
// Opens a database by using a named connection string.
var db = Database.Open("SmallBakeryConnectionString");
```

    Database.OpenConnectionString(connectionString)

Opens a database using the connection string. (This contrasts with
`Database.Open`, which uses a connection string name.)

```c#
var db = Database.OpenConnectionString("Data Source=|DataDirectory|\SmallBakery.sdf");
```

### `Database.Query`

    Database.Query(SQLstatement[,parameters])

Queries the database using `SQLstatement` (optionally passing parameters)
and returns the results as a collection.

```
foreach (var result in db.Query("SELECT * FROM PRODUCT")) {<p>@result.Name</p>}

foreach (var result = db.Query("SELECT * FROM PRODUCT WHERE Price > @0", 20))
   { <p>@result.Name</p> }
```

### `Database.QuerySingle`

    Database.QuerySingle(SQLstatement [, parameters])

Executes `SQLstatement` (with optional parameters) and returns a single
record.

```c#
var product = db.QuerySingle("SELECT * FROM Product WHERE Id = 1");
var product = db.QuerySingle("SELECT * FROM Product WHERE Id = @0", 1);
```

### `Database.QueryValue`

    Database.QueryValue(SQLstatement [, parameters])

Executes `SQLstatement` (with optional parameters) and returns a single value.

```c#

var count = db.QueryValue("SELECT COUNT(*) FROM Product");
var count = db.QueryValue("SELECT COUNT(*) FROM Product WHERE Price > @0", 20);
```


[win-build-badge]: https://img.shields.io/appveyor/ci/raboof/notwebmatrix.data/master.svg?label=windows
[win-builds]: https://ci.appveyor.com/project/raboof/notwebmatrix.data
[nix-build-badge]: https://img.shields.io/travis/atifaziz/NotWebMatrix.Data/master.svg?label=linux
[nix-builds]: https://travis-ci.org/atifaziz/NotWebMatrix.Data
[myget-badge]: https://img.shields.io/myget/raboof/vpre/NotWebMatrix.Data.svg?label=myget
[edge-pkgs]: https://www.myget.org/feed/raboof/package/nuget/NotWebMatrix.Data
[nuget-badge]: https://img.shields.io/nuget/v/NotWebMatrix.Data.svg
[nuget-pkg]: https://www.nuget.org/packages/NotWebMatrix.Data

[ms-wmd]: https://docs.microsoft.com/en-us/aspnet/web-pages/overview/api-reference/asp-net-web-pages-api-reference#data
[aspnet-wp]: https://docs.microsoft.com/en-us/aspnet/web-pages/
[aspnet-wm]: https://en.wikipedia.org/wiki/ASP.NET_Web_Matrix
[netstd]: https://docs.microsoft.com/en-us/dotnet/standard/net-standard
[orig-ref-doc]: https://github.com/aspnet/Docs/blob/d157e2ec7a373f9cd7df9dc0d2e360fa018bc1b1/aspnet/web-pages/overview/api-reference/asp-net-web-pages-api-reference.md#data
