# Timesheet

Even employees with trust-based working hours need to track times.
With a small script, calling a webpage inserts the current timestamp
in a database to track clock-in and clock-out timestamps.

## URLs

* [PhpMyAdmin](https://w0063a67.kasserver.com/mysqladmin/PMA4/sql.php?db=d03ca9da&table=timesheet&pos=0&server=67100056)
* [UpdateDB](https://timesheet.rottmann.it/e21bbb7f-1f99-46cc-bdac-bb3da8df2c06.php)

## PHP Insert Timestamp

```php
<?php
$servername = "localhost";
$username = "d03ca9da";
$password = "";
$dbname = "d03ca9da";

// Create connection
$conn = new mysqli($servername, $username, $password, $dbname);
// Check connection
if ($conn->connect_error) {
  die("Connection failed: " . $conn->connect_error);
}

$sql = "INSERT INTO `timesheet` (`idt`, `ts`) VALUES (NULL, CURRENT_TIMESTAMP)";

if ($conn->query($sql) === TRUE) {
  echo "New record created successfully";
} else {
  echo "Error: " . $sql . "<br>" . $conn->error;
}

$conn->close();
?> 
```

## Database Schema

```sql
create table timesheet
(
    idt int auto_increment
        primary key,
    ts  datetime default CURRENT_TIMESTAMP not null
);
```

##