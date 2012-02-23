try {
	
	Add-Type -Path (join-path (Split-Path $MyInvocation.MyCommand.Definition -Parent -ErrorAction silentlycontinue) "System.Data.SQLite.dll");
} catch {
	$_ | write-error;
	return;
}

function Create-RRD {
<#
.SYNOPSIS
Creates an RRD file.
.DESCRIPTION
Create an initial RRD file with time (in seconds) resolution and number of entries.
.PARAMETER File
Path to the file you wish to create.
.PARAMETER Resolution
Number of seconds between updates.
.PARAMETER Entries
Number of datapoints to keep track of.
.PARAMETER Fields
An array of names that you want to use.
.PARAMETER StartTime
The starting time of the data tracked.
#>
	param(
		[parameter(Mandatory=$true)]$File
		,[int]$Resolution = 300
		,[int]$Entries = 12
		,[parameter(Mandatory=$true)][string[]]$Fields
		,[datetime]$StartTime = ([datetime]::Now)
	)

	try {
		$conn.Close();
	} catch {}

	$conn = New-Object system.data.sqlite.sqliteconnection "data source=$File;Version=3;";
	$conn.Open();
	$cfields = "";
	$cfields_empty = ",''" * $Fields.Length;
	foreach($f in $Fields) {
		$cfields += ("`t,{0} TEXT" + [environment]::NewLine) -f $f;
	}
	$sql = @"
DROP TABLE IF EXISTS round_table;
CREATE TABLE IF NOT EXISTS round_table (
	time_id INT
	$cfields
);
DROP TABLE IF EXISTS meta;
CREATE TABLE IF NOT EXISTS meta (
	res INT
	,entries INT
	,starttime INT
	,lasttime INT
	,currentROWID INT
);
INSERT INTO meta VALUES ($Resolution, $Entries, $($StartTime.ToFileTime()), 0, 0);
"@;
	$sql;
	$cmd = New-Object data.sqlite.sqlitecommand $conn;
	$cmd.CommandText = $sql;
	$cmd.ExecuteNonQuery() | Out-Null;

	$cmd.Transaction = $conn.BeginTransaction();

	$cmd.CommandText = "INSERT INTO round_table VALUES (@tid $cfields_empty)";
	for($i = 0; $i -lt $Entries; $i++) {
		$cmd.Parameters.Clear() | Out-Null;
		$cmd.Parameters.add((New-Object data.sqlite.sqliteparameter "@tid", ($StartTime.AddSeconds(-($i*$Resolution)).tofiletime()) )) | Out-Null;
		$cmd.ExecuteNonQuery() | Out-Null;
	}
	$cmd.Transaction.Commit() | Out-Null;
	$conn.close();
}

function Update-RRD {
<#
.SYNOPSIS
Update an RRD file with data.
.DESCRIPTION
Updates RRD file based on time specified. The row updated is the nearest to the specified time.

For instance, if we have a 5m resolution and we update 1m after the last, we will overwrite the previous data. If update happens on or after 2.5m, data will be saved at the next time point.
.PARAMETER File
RRD file to work with.
.PARAMETER Timestamp
The timestamp of this update. The nearest time index will be updated.

Default is now.
.PARAMETER DataHashTable
HashTable of data to update.

@{"fieldname1" = data, "fieldname2" = data2, ...};
#>
	param(
		[parameter(Mandatory=$true)]$File
		,$Timestamp = ([datetime]::Now)
		,[parameter(Mandatory=$true)][HashTable]$DataHashTable
	);

	try {
		$conn.Close();
	} catch {}

	$conn = New-Object system.data.sqlite.sqliteconnection "data source=$File;Version=3;";
	$conn.Open();
	$cmd = New-Object data.sqlite.sqlitecommand $conn;

	$cmd.CommandText = "SELECT *, (SELECT time_id FROM round_table ORDER BY time_id DESC LIMIT 1) AS last_time_id FROM meta;";
	$metares = $cmd.ExecuteReader();
	while($metares.Read()) {
		$config = New-Object psobject -Property @{
			"res" = $metares.GetInt64(0);
			"entries" = $metares.GetInt64(1);
			"starttime" = $metares.GetInt64(2);
			"lasttime" = $metares.GetInt64(3);
			"lasttimeid" = $metares.GetInt64(5);
			"currentROWID" = $metares.GetInt64(4);
			#"NextROWID" = -1;
		};
	}
	$metares.Close();

	Add-Member -Force -InputObject $config -MemberType ScriptProperty -Name res_filetime -Value {
		$this.res * [math]::Pow(10, -17);
	}

	#$n = ([datetime]"2/7/2012 9:30am");
	#$n = ([datetime]"2/7/2012 9:45am");
	#$n = [datetime]::Now;
	#$n = ([datetime]"2/7/2012 10:00am");
	$n = $Timestamp;
	$time_id = [datetime]::FromFileTime($config.starttime).addseconds(
		[math]::round(
			(([datetime]::FromFileTime($n.ToFileTime() - $config.starttime) - [datetime]::FromFileTime(0)).totalseconds)/$config.res
		,0)*$config.res
	).tofiletime();

	Add-Member -Force -InputObject $config -MemberType NoteProperty -Name current_time_id -Value $time_id;

	Add-Member -Force -InputObject $config -MemberType ScriptProperty -Name nextStaticROWID -Value {
		return $this.currentROWID + 1;
	}

	Add-Member -Force -InputObject $config -MemberType ScriptProperty -Name id_diff -Value {
		(([datetime]::FromFileTime($time_id) - [datetime]::FromFileTime($this.lasttimeid)).totalseconds/$this.res)
	}

	Add-Member -Force -InputObject $config -MemberType ScriptProperty -Name id_diff_gt_entries -Value {
		$this.id_diff -ge $this.entries;
	}
	Add-Member -Force -InputObject $config -MemberType ScriptProperty -Name id_diff_nextROWID -Value {
		$t = ($this.currentROWID + $this.id_diff)%$this.entries;
        if($t -lt 1) {
            $t = $this.entries;
        }
        return $t;
	}
	Add-Member -Force -InputObject $config -MemberType ScriptProperty -Name id_diff_wrap -Value {
		($this.currentROWID + $this.id_diff) -gt $this.entries;
	}

	###END CONFIG

	$config

	$update_seq = @();
	for($i = 0; $update_seq.Count -lt $config.id_diff -and $update_seq.Count -le $config.entries; $i++) {
		$t = ($config.id_diff_nextROWID - $i);
		if($t -lt 1) {
			$t = $config.entries + $t;
		}
		$update_seq += $t;
	}

	#$update_seq;

	#(300*[math]::Pow(10, -17))
	<#
	Next insert time
	[datetime]::FromFileTime($config.starttime).addseconds(
		[math]::Ceiling(
			(([datetime]::FromFileTime([datetime]::now.ToFileTime() - $config.starttime) - [datetime]::FromFileTime(0)).totalseconds)/300
		)*300
	)

	closest insert time

	[datetime]::FromFileTime($config.starttime).addseconds(
		[math]::round(
			(([datetime]::FromFileTime([datetime]::now.ToFileTime() - $config.starttime) - [datetime]::FromFileTime(0)).totalseconds)/300
		,0)*300
	)

	#>

	$cfields_update = @("time_id = $time_id");
	$cfields_update_null = @(); #@("time_id = $time_id");
	foreach($k in $DataHashTable.Keys) {
		$cfields_update += @("{0} = @{0}" -f $k);
		$cfields_update_null += @("{0} = NULL" -f $k);
	}

	#$cfields_update = [string]::Join(", ", $cfields_update);
	#$cfields_update_null = [string]::Join(", ", $cfields_update_null);
	#$cmdb = New-Object data.sqlite.sqlitecommandbuilder (New-Object data.sqlite.sqliteDataAdapter "SELECT ROWID, * FROM round_table;", $conn)
	#$insert = $cmdb.GetInsertCommand();
	if($config.currentROWID -gt $config.entries) {
		$config.currentROWID = 1;
	}

	#$cmd.CommandText = "SELECT time_id FROM round_table ORDER BY time_id DESC LIMIT 1;";
	$resp = $cmd.ExecuteScalar();
	$cmd.CommandText = "";
	#if($timestamp.GetType().equals([datetime])) {
		
	#} else {
		#if($index -eq -1 -or $index -gt $config.entries) {
			$cmd.CommandText = "UPDATE round_table SET {0} WHERE ROWID = $($config.id_diff_nextROWID);`r`n" -f ([string]::join(", ", [string[]]$cfields_update)); # AND time_id = $time_id
			
		#}
	#}

	$new_currentROWID = ($config.currentROWID + $config.id_diff)%$config.entries;
	
	<#if($config.id_diff -ge $config.entries) {
		$overwriteCount = $config.entries;
	} else {
		$overwriteCount = [math]::Abs($config.id_diff - $config.currentROWID)
	}#>

	if($config.id_diff - $config.currentROWID -gt 1) {
		$ids_to_update = @();
		#for($i = $overwriteCount; $i -gt 1; $i--) {
		for($i = 1; $i -lt $update_seq.Count; $i++) { #skip the first since that's the row that'll contain data.
			$nullrowid = $update_seq[$i];
			$rowtimestamp = [datetime]::FromFileTime($config.current_time_id).addseconds(-($config.res * $i)).tofiletime();
			<#$a = $config.currentROWID - $i + 2;
			if($a -lt 0) {
				$a = 10 + $a;
			}
			$ids_to_update += $a;
			$a#>
			#$cmd.CommandText += "UPDATE round_table SET time_id = $a, $cfields_update_null WHERE ROWID = $($a);`r`n";
			$cmd.CommandText += ("UPDATE round_table SET time_id = {0}, {2} WHERE ROWID = {1};`r`n" -f $rowtimestamp, $nullrowid, ([string]::join(", ", [string[]]$cfields_update_null)));#([string]::Join(", ", $cfields_update_null)));
			
		}
		<#for($i = $new_currentROWID; $ids_to_update.length -lt [math]::Abs(($config.id_diff % 10) - $config.currentROWID); $i++) {
			if($i -ge $config.entries) {
				$i = 9;
			}
			$ids_to_update += $i;
			$i;
		}#>
	}

	$cmd.CommandText += "UPDATE meta SET currentROWID = $($config.id_diff_nextROWID), lasttime = $($timestamp.toFileTime());";
	$cmd.CommandText;
	#$cmd.Parameters.Add((New-Object Data.Sqlite.SqliteParameter , 
	foreach($dhtk in $DataHashTable.Keys) {
		$cmd.Parameters.Add((New-Object Data.Sqlite.SqliteParameter $dhtk, $DataHashTable[$dhtk])) | Out-Null;
	}
	$cmd.Prepare();
	$cmd.ExecuteNonQuery();
	$conn.Close();
}

function Fetch-RRD {
<#
.SYNOPSIS
Fetches data from an RRD file based on time given.
.DESCRIPTION
Fetch data form an RRD file based on specified time. Default timeframe is now-1d starting and now for the end.
.PARAMETER File
The target RRD file.
.PARAMETER Start
Starting time for data set
.PARAMETER End
Ending time for data set
.PARAMETER Filter
Pre-return data manipulation. Types: diff.

Diff: difference between a row and the previous entry. Useful for switch port usage.
#>
	param(
		[parameter(Mandatory=$true)]$File
		,[datetime]$Start = ([Datetime]::Now.AddDays(-1))
		,[datetime]$End = ([Datetime]::Now)
		,[string]$Filter = "none"
	);
	$conn = New-Object system.data.sqlite.sqliteconnection "data source=$File;Version=3;";
	$conn.Open();
	$cmd = New-Object data.sqlite.sqlitecommand $conn;
	
	$cmd.CommandText = "PRAGMA table_info(round_table);";
	$metares = $cmd.ExecuteReader();
	$rt_info = @();
	while($metares.Read()) {
		$t = New-Object psobject -Property @{
			"name" = $metares.getString(1);
            "type" = $metares.getString(2);
		};
        switch($t.type) {
            "text" {
                $t.type = "string";
            }
            "int" {
                $t.type = "int64";
            }
        }
        $rt_info += $t;
	}
	#$metares.Close();
    $dr = @();
    $cmd = New-Object data.sqlite.sqlitecommand $conn;
	switch($Filter) {
		"diff" {
			$sql = @"
SELECT 
	c AS id
	,time_id
	,{0}
FROM (
	SELECT
		time_id
		,CASE WHEN ROWID - 1 < 1 THEN (SELECT entries FROM meta LIMIT 1) ELSE ROWID - 1 END AS p
		,ROWID AS c
		,CASE WHEN ROWID + 1 > (SELECT entries FROM meta LIMIT 1) THEN 1 ELSE ROWID + 1 END AS n
	FROM round_table
	);
    WHERE
        time_id >= {1}
        AND time_id <= {2}
"@;
			$fsql = ($rt_info | ?{!($_.name -match "time_id")} | %{"(SELECT {0} FROM round_table WHERE ROWID = n) - (SELECT {0} FROM round_table WHERE ROWID = c) AS diff_{0}`r`n" -f $_.name });
			$cmd.CommandText = $sql -f ([string]::join(",", [string[]]$fsql)), $Start.ToFileTime(), $end.ToFileTime();
			#$cmd.commandtext
			$metares = $cmd.ExecuteReader();
			while($metares.Read()) {
				$t = new-object object[] $metares.FieldCount;
				$t2 = new-object psobject;
				#$t = $null;
				for($i = 0; $i -lt $t.length; $i++) {
					#$t[$i] = $metares.GetFieldType($i).fullname;
					$m = "`$metares.get{0}(`$i);" -f $metares.GetFieldType($i).name;
					try {
						#$t2.add($metares.getName($i), (invoke-expression $m));
						switch($metares.getName($i)) {
							"time_id" {
								add-member -force -input $t2 -membertype noteproperty -name $metares.getName($i) -value $metares.GetInt64($i);
							}
                            "id" {
                                add-member -force -input $t2 -membertype noteproperty -name $metares.getName($i) -value $metares.GetInt64($i);
                            }
							default {
								add-member -force -input $t2 -membertype noteproperty -name $metares.getName($i) -value (invoke-expression $m);
							}
						}
					} catch{ 
						add-member -force -input $t2 -membertype noteproperty -name $metares.getName($i) -value $metares.getValue($i);
					}
				}
				#$t2.time_id = [uint64]$t2.time_id;
				#$t = $metares.FieldCount;
				$dr += $t2;
			}
			$metares.Close();
			$conn.Close();
			break;
		}
		default {
			
			$cmd.CommandText = "SELECT {0} FROM round_table WHERE time_id >= {1} and time_id <= {2};" -f ([string]::join(",", ($rt_info |%{$_.name}))), $Start.ToFileTime(), $End.ToFileTime();
			$metares = $cmd.ExecuteReader();
			
			while($metares.Read()) {
				$props = @{};
				foreach($fn in $rt_info) {
					try {
						$mri = [array]::IndexOf($rt_info, $fn);
						$method = "get{0}" -f $fn.type;
						$t = Invoke-Expression ('$metares.{0}($mri)' -f $method);
						$props.add($fn.name, $t);
					} catch {
						$props.add($fn.name, $metares.GetValue($mri));
					}
				}
				$dr += New-Object psobject -Property $props;
			}
			$metares.Close();
			$conn.Close();
			foreach($v in $dr) {
				$v.time_id = [datetime]::FromFileTime($v.time_id);
			}
		}
	}
    return $dr;
}

function Reindex-RRD {
	param(
		[parameter(Mandatory=$true)]$File
	);
	
	
}

export-modulemember -function *-RRD