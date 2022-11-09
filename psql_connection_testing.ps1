$ConnectionString = 'DSN=PostgreSQL35W'
$conn = New-Object System.Data.Odbc.OdbcConnection;
$conn.ConnectionString = $ConnectionString
$conn.Open()
$query = "SELECT id, name FROM terms;"
$cmd = New-object System.Data.Odbc.OdbcCommand($query,$conn)
$ds = New-Object system.Data.DataSet
(New-Object system.Data.odbc.odbcDataAdapter($cmd)).fill($ds) | out-null
$conn.close()
foreach($rec in $ds.Tables[0])
{
    $rec.id
    $rec.name
}

