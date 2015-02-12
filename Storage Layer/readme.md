# The Persistent Staging Area ("PSA")	
----------
## {1} PSA Overview:

The PSA is the first area in the multiple tier EDW solution, and it primary purpose is to hold source system equivalent data in a temporal fashion – ensuring heavy downstream analytics do not hinder the transactional based source systems.  This tier houses all source entities (tables, views, XML, etc.) determined appropriate by any approved business case, which includes, in their entirety, all the attributes associated with the given entity.  Each attribute is stored in their native format, making common data elements heterogeneous across entity sources .  Additionally, generational history is maintained; in turn, traditional temporal metadata is captured and stored as well, e.g. the record creation date.  This generational history and metadata allows for non-volatile and non-destructive properties to ensure a complete understanding of how information changes over time.  The diagram below shows the simple flow from the source systems through the transformational layer and into the PSA.  The logic for actually loading the information into the PSA is covered later in this section, and more detail can also be found later in the ‘Transformational Layer (“ETL”)’ section.

![alt text](https://github.com/mjfii/Persistent-Staging-Area/raw/master/images/psa_fig_1.png "PSA ETL Flow Example")

What follows in this section is relevant only to the PSA layer of the EDW.  Other layers will maintain their own methodology and patterns in conjunction with the appropriate use-case, purpose, and role in the larger solution.

## {2} Database Definition:
This section will cover the configurations and logical definition of the PSA database.  The PSA layer will be encapsulated by a single database residing on the EDW instance.  The database name is [edw_psa], and can be referenced as such across the instance.

### {2.1}	Database Configurations:
The following is by no means meant to be an exhaustive list of database configurations, but rather a list common elements that may be altered to support a business and/or use case, while still maintaining best practices.

Configuration | Value
--- | --- 
Transparent Data Encryption ("TDE"): | Not Enabled
Snapshot Isolation | Not Enabled
Collation | Latin1_General_100_CI_AS
Service Broker | Not Enabled
Recovery Model | Full
Change Tracking | Enabled
Filestream | Not Enabled

### {2.2} File Groups:
The different components of each PSA entity have been broken up to assist in rapid extraction, archiving, and data manipulation.  The different File Groups for the PSA database can be found below.

Name | File Count | Read-Only | Default | Description
--- | --- | --- | --- |---
PRIMARY | 1 | No | Yes | Stores master and system objects, i.e. object definition, statistics, etc.
SPK | 1	 | No | No | Stores the non-clustered primary key constraints for each entity.
SNK | 1	 | No | No | Stores the non-clustered unique alternate key for each entity.
ACTIVE | 1 | No | No | Stores the clustered and active records for each entity.
ARCHIVE | 1 | No | No | Stores the clustered and archived records for each entity.
TMP | 1	 | No | No | Stores temporary tables and queues for processing PSA methods.

### {2.3}	Physical Files:
The PSA will maintain a one-to-one relationship between File Groups and the physical database files themselves.  Each of the files can be found below.

Name | File Group | Physical Name | Type
--- | --- | --- | ---
psa_primary | PRIMARY | psa_primary.mdf | Rows
psa_spk | SPK | psa_spk.ndf | Rows
psa_snk | SNK | psa_snk.ndf | Rows
psa_active | ACTIVE | psa_active.ndf | Rows
psa_archive | ARCHIVE | psa_archive.ndf | Rows
psa_tmp | TMP | psa_tmp.ndf | Rows
psa_log |  | psa_log.ldf | Log

### {2.4}	Physical File Locations:
Depending on the environment of deployment, the physical files may be located in different directory locations, LUNS, and/or SAN allocations.  The path for each file for each environment can be found below.  However, the extended path to the below will always be ‘\edw_psa\ [Physical File Name]’.  The ‘Servers & Disk Specifications’ are discussed in a later section.

Name | Dev | Test | Prod | QA
--- | --- | --- | --- | ---
psa_primary | | | | 
psa_spk | | | | 
psa_snk | | | | 
psa_active | | | | 
psa_archive | | | | 
psa_tmp | | | | 
psa_log | | | | 

## {3}	Source System & Schema Alignments:
Each source system used to populate the PSA will receive a four (4) digit alpha code that will be aligned directly with a database schema name.  The alignment between the two can be found below .

Source System | Schema | Source Technology | Frequency of Load (Expected Latency)
--- | --- | --- | ---
 | | | 
 | | | 

### {3.1}	Schema Ownership:
All schemas will be owned by the `[psa_owner]` database role.  More information regarding PSA security and ownership can be found in the ‘Security’ section.

### {3.2}	Schema Sequences:
A single database sequence is bound to each schema to allow for unique surrogate keys.  All DDL will reference these sequences for incrementation, and they will hold one thousand (1,000) values in cache for speed of delivery to the appropriate entity.  Assuming the maximum value is reached, the values will recycle back to 1.  The definition can be seen below.

```SQL
	create sequence [ERPS].[SPK] 
	start with 1 increment by 1 minvalue 1 cycle cache 1000;
```

You can view each of the sequences and their definition with the below query.

```SQL
	select * from sys.sequences where [name] like N'%';
```

## {4}	Entity Object Definition:
For every entity in the PSA, notwithstanding the source content and make-up, the physical build will be the same.  Every entity will have the same naming convention as `[Source System].[Logical Entity Name]` and can be accessed as such.  While there is a different logical name for each entity, the attribute names and data types will be the exact same as they reside in the source system.

### {4.1}	Surrogate Primary Key ("SPK")
Each entity will have a unique incrementing column that will be defined as a `bigint`.  This identity column will have a primary key constraint applied to it, and it alone.  To allow for this incrementation, the SPK uses the Schema Sequences noted in a prior section.  This constraint will not be clustered and will be placed on the SPK File Group.  You can see the constraints with the following query.

```SQL
    select *
    from sys.key_constraints 
    where [name] like N'%{Surrogate Primary Key}';
```

### {4.2}	Source Natural Key ("SNK"):
The SNK is defined as the source entity’s primary key and the change increment counter that is defined in the next section.  Aside from the incrementation, the SNK may be a simple integer, or a complex composite key derived from many attributes (columns).  The PSA will house the SNK in a form as close, if not exact, to the original source as possible . Furthermore, this document may reference the SNK both with and without the change increment, since the former is just a temporal representation of the latter.

Additionally, a table constraint, or alternate key, will be placed on each entity to ensure the source uniqueness is maintained.  This constraint will not be clustered and will be placed on the SNK File Group.  You can see the constraints with the following query.

```
	select *
    from sys.key_constraints 
    where [name] like N'%{Source Natural Key}';
```

### {4.3}	Control Attributes:
Each PSA entity will contain the below control attributes that facilitate temporal tracking, deletions, archiving, hashing, etc.  All of the attributes are controlled internally and cannot be manually manipulated.  It is very important to note that there are not any logical ‘updates’ or ‘deletes’ per se, but, rather, an ‘insert’ of new information or desired ‘updates’ or ‘deletes’ with a change incrementation.  This is conceptually the same as managing a Type II Slowly Changing Dimension ("SCD") that might be found in a data mart.  The control flow is discussed in more detail in a later section.

Attribute | Data Type | Default Value | Description
--- | --- | --- | ---
psa_entity_key | bigint | 1 | This columns acts as the Surrogate Primary Key SPK; it is unique unto itself.  The value is incremental; it starts and 1 and will increase to the maximum capacity of ‘bigint’.
psa_parent_entity_key | bigint | [psa_entity_key] | The parent acts as a surrogate grouping key.   Upon INSERT of a record, the value is the same as the [psa_entity_key].  Through life of the record via UPDATEs and DELETEs, the [psa_entity_key] is carried through in this control attribute.
psa_entity_sequence | smallint | 1 | In conjunction with the SNK, the sequence is a change increment as the records are altered.
psa_start_period | datetime2(7) | sysutcdatetime() | A time stamp recording when the record is valid ‘FROM’.
psa_end_period | datetime2(7) | null | A time stamp recording when the record is valid ‘TO’.  This attribute may be null until an alteration has been made.
psa_active_state | bit | 1 | A binary Boolean value that determines if a record has been deleted. 1=Yes and 0=No, or, 1=Active and 0=Deleted.  
psa_dml_action | nvarchar(1) | “I” | A persisted computed column that identifies the DML action that has been made to any given record.
psa_current_flag | bit | 1 | A persisted computed column that identifies whether or not the record is the most recent representation.
psa_archive_flag | bit | 0 | A binary Boolean value that determines if a record has been archived (moved to an alternate partition). 1=Yes and 0=No, or, 1=Archive and 0=Active.  
psa_batch_id | uniqueidentifier | null | A GUID type field that references the ETL logging batch.  This may be null should the load be ad-hoc or non-batch related.
psa_hash_id | varbinary(20) |  | A 20 byte hex field that maintains an SHA1 hash that is used for comparison during DML actions.

### {4.4}	Partitioning & Clustered Indexes:
Partitioning tables plays an important role in archiving old or unused information.  Record sets, i.e. SNK’s, that have been deemed irrelevant can be moved from an active state to an archived state, which will move the record to an alternate table partition prior to a physical ‘hard’ deletion of the record.  The entities are partitioned using the ‘psa_archive_flag’ control attribute, and since this attribute is a binary Boolean value, there will only be two physical partitions, which are defined using the database partition function ‘ReadyForArchive’.  The definition for the partition function is below.

```SQL
	-- psa entity partition function
	create partition function [ReadyForArchive](bit) as range left for values (0);
```

In order to keep the archived records offset from the active records, the database partition scheme ‘ArchiveFileGroupAssignment’ allocated the flagged (1) records to the ARCHIVE File Group and the non-flagged (0) records to the ACTIVE File Group.  The definition for the partition scheme is below.

```SQL
	-- psa entity partition scheme
	create partition scheme [ArchiveFileGroupAssignment] as 
	partition [ReadyForArchive] to ([ACTIVE],[ARCHIVE]);
```

Furthermore, in line with best practices, a (the) clustered, and non-unique, index is placed on each of the entities.  In addition to the `[psa_archive_flag]` control attribute being used as the first ordinal in descending order, the SNK is defined using its natural ordinals and sorting directly after the archiving flag.  The index is then placed on the ‘ArchiveFileGroupAssignment’ partition scheme.  An example of a clustered index can be see below.

```SQL
	-- example of a clustered psa entity clustered index
	create clustered index [cx : ERPS.TransactionHeader {Archival Planning}] 
	on [ERPS].[TransactionHeader] 
	 (
	   [psa_archive_flag] desc,
	   [TransactionNumber] asc, -- SNK(1)
	   [TransactionLine] desc, -- SNK(2)
	   [psa_entity_sequence] desc-- SNK(3)
	 ) with (data_compression=row,fillfactor=80) 
	on [ArchiveFileGroupAssignment]([psa_archive_flag]);
```

Each for the clustered indexes for the PSA entities can be found using the following query.

```SQL
	-- clustered indexes
	select * from sys.indexes where [type]=1 and [name] like N'cx%';
```

Archiving differs from a deletion in the source systems, which is really transcended as a ‘soft’ delete in the PSA; a ‘soft’ delete is a representation of a DML action and archiving is a processing tool for database administration and data governance .  

### {4.5} Entity Trigger:
The primary database object used to track temporal alterations in the PSA is the ‘AFTER UPDATE’ trigger on each of the entities.  After any given (and latest) record is updated, the cached values of both the new and old values are consolidated and inserted as a new record, which is now defined as non-current and accessible only through a temporal abstraction or query.  These triggers can be found with the following query.

```SQL
	-- after update trigger
	select * from sys.triggers where [name] like N'%{Temporal Governor}';
```

An example pattern of the trigger DDL can be found in the ‘Example DDL’ Appendix.

### {4.6}	Entity Statistics:
Statistics play a very important role in the query engine optimization, primarily in terms of complex and/or large extractions.  And, depending on the downstream use of information, PSA queries could be very expensive.  In turn, statistics have been predefined on all control attributes and all SNK attributes where the ordinal position of the alternate key is NOT one (1).  The naming convention is: `[st : schema.entity :: attribute name]`.  You can view the predefined statistics with the following query.

```SQL
	-- statistics
	select * from sys.stats where name like N'st%';
```

Statistics should be updated regularly in adherence with the EDW maintenance plan.  

### {4.7}	Entity Change Tracking:
In order to capture changes to entities in the PSA, Change Tracking ("CT") will be enabled on the database.  Turning on the change tracking is done with the following script.

```SQL
	if not exists
	 (
	   select * 
	   from sys.change_tracking_databases 
	   where database_id=db_id(N'edw_psa')
	 )
	alter database [edw_psa] set change_tracking=on
	 (
	   change_retention=4 days,
	   auto_cleanup=on
	 );
```

Since CT is a synchronous feature, the additional overhead is roughly equivalent to the addition of a single new index to each entity.  The change retention will be set to four (4) days to ensure that any issues over weekends or holidays are accounted for.  This enablement is very important to limit downstream data flows to only incremental changes in content.  More information on utilizing CT in the PSA, can be found in the ‘Changes’ Abstraction section.

### {4.8} Other Configurations & Objects:
Checks and Defaults:  Each entity will have two (2) checks and five (5) defaults, all of which are assigned to the control attributes.

* Temporal Sequence: A check to ensure the start date of a record is prior to the end date of the record.  The name has a suffix of ‘{Temporal Sequence On Current Period}’.
* Positive Incrementation: A check to ensure incrementation values are greater than 1.  The name of the suffix has {Positive Incrementation}.
* Default Archive: A default of ‘False’ to the value of a new record being placed on the archive partition.  The name has a suffix of ‘{Archive Record Flag}’.
* Record State: A default of ‘True’ to the value of a new record NOT being deleted immediately upon INSERT.  The DELETE DML language must be called to alter the flag. The name has a suffix of ‘{New Record State}’.
* New Change Sequence: A default of 1 to the value of the temporal sequence counter applied to each SNK.  The name has a suffix of ‘{Change Incrementation Sequence Number}’.
* New Entity Sequence: A default to the next value of schema SPK database sequence object. The name has a suffix of ‘{SPK Sequence Number}’.
* New Parent Entity Sequence: A default to the ‘New Entity Sequence’ value of the inserted record.  The name has a suffix of ‘{Parent SPK Sequence Number}’.

You can view the checks and defaults with the following query.

```SQL
	-- checks
	select * from sys.check_constraints where name like N'ck%';
	
	-- defaults
	select * from sys.default_constraints where name like N'df%';
```

Data Compression: In order to temper the server I/O and the disk storage, row level data compression is used on the SNK, SPK, and clustered index.

Fill Factors:  Fill Factors on the SNK and clustered index are predefined at 80%, and 95% on the SPK due to its incremental nature.  These are simply practical (and common) starting points which should be revisited, and potentially adjusted, during ongoing and formal database management.

## {5}	System Object Definitions:
The PSA database will retain eight (8) system objects that will perform a variety of administrative roles and/or features.  These objects will all be part of the [dbo] schema and securitized accordingly.  The definitions to these objects can be found in the DDL Library.  

Object Name | Type | Description
--- | --- | --- 
psa_change_tracking_entity_version | table | A table that manages the change tracking integer of the existing pull and last pull of the SQL Server Change Tracking versions.
psa_get_change_tracking_entity_version | procedure | A procedure that ‘gets’ the latest change tracking version for a specified entity.  This procedure will be called frequently from ETL packages.
psa_set_change_tracking_entity_version | procedure | A procedure that ‘sets’ the latest change tracking to ‘complete’.  This procedure will be called frequently from ETL packages.
psa_active_vs_archive | view | A view that displays ‘active’ versus ‘archived’ records for each entity.
psa_drop_entity | procedure | A procedure that drops any given entity and all related abstractions and methods.  It really drops objects; so, use with care.

## {6} Entity Abstractions and Methods:
Each entity residing in the PSA will contain ten (10) abstractions and/or methods.  The definitions for each are stated below; however, they are designed primarily for ease of use, mainly temporal extraction, ETL processing, and archiving.  Furthermore, the defined pattern for each abstraction and method is the exact same across all entities.

As stated in a prior section, each entity can be defined as ‘[Source System].[Logical Entity Name]’.  Since each entity has temporal tracking attributes, and in order to extend the ease of use, with respect to ‘reads’ and ‘writes’, the abstracts and methods can be referenced as ‘[Source System].[Logical Entity Name.Abstract | Method Name]’.  These database objects do not physically store permanent data as they are views, table-valued functions, temporary tables , or stored procedures.

An example pattern of the trigger DDL can be found in the ‘Example’ DDL Appendix.

Note: the actual flow of these patterns are documented further in the ‘Data Manipulation Logic and Flow’ documentation of the PSA.

### {6.1} ‘Control’ Abstraction:
The ‘Control’ abstraction is the primary driver and interface for Data Manipulation Language (“DML”) with respect to each entity.  It resides in the PSA as a database view with a predicate limiting the set to only the most recent values of the record, and it includes records that have been deleted in (from) the source system.  INSERT, UPDATED, and DELETE statements must be executed against THIS abstraction in order to capture the appropriate temporal tracking attributes.  This capture is mechanized through very simple INSTEAD OF triggers .  Examples for using this abstraction are below.

```SQL
	-- select
	select * from [ERPS].[TransactionHeader.Control];
	
	-- insert
	insert [ERPS].[TransactionHeader.Control] ([TransactionNumber],[Amount]) 
	values (1,5.5),(2,6.5);
	
	-- update
	update [ERPS].[TransactionHeader.Control] set [Comments]=N'example';
	
	-- delete
	delete [ERPS].[TransactionHeader.Control];
```

The INSTEAD OF triggers allows for the ability to flag deletions versus an actual ‘hard’ delete, disallow an update of any given SNK, and govern the contiguous time periods.  More information can be found in the ‘Data Manipulation Logic and Flow’ section; however, below if a simplification of the INSTEAD OF trigger actions.

DML Action | Logical Action
--- | ---
INSERT | Pass through the new cached values with the addition of the required control attributes in the form of an INSERT.
UPDATE | Pass through the new cached values in conjunction with the old cached values (for a contiguous effect), and include the required control attributes in the form of an UPDATE.  *Important Note:* any update to an entity SNK that has been flagged as ‘deleted’ or ‘archived’ will reinstate the record as ‘active’.
DELETE | Pass through the old cached values with an update to the ‘psa_active_state’ control attribute in the form of an UPDATE.

Columns include: SNK, related attributes.

### {6.2} ‘AsIs’ Abstraction:
The ‘AsIs’ abstraction is a database view and is very similar to the ‘Control’ abstraction with three (3) notable differences.  First, only current records are included, meaning any record that has been flagged as ‘deleted’ or ‘archived’ will not appear in this abstraction.  Second, there are no capabilities to handle DML, i.e. it is for extraction purposes only.  And, finally, the abstraction does not consider archived records.  An example is below.

```SQL
	-- select
	select * from [ERPS].[TransactionHeader.AsIs];
```

Columns include: SNK, related attributes.

### {6.3} ‘AsWas’ Abstraction:

The ‘AsWas’ abstraction is the last temporal database view, and instead of returning the most recent and current result set, this abstraction show the entire life (with the exception of ‘archived’ records) of any given record with change incrementation, linear timestamps, and the type of modification conducted.  Again, there are no capabilities to handle DML.

```SQL
	-- select
	select * from [ERPS].[TransactionHeader.AsWas];
```

Columns include: SNK, related attributes, Sequence Number, DML Action, Valid-From date, and Valid-To date.

### {6.4} ‘AsOf’ Abstraction:

The ‘AsOf’ abstraction is the only table-valued function.  It accepts a single UTC ‘datetime2’ data type with a precision of 7, or 100 nanoseconds.  It pulls the exact same result set as the ‘AsIs’ abstraction, with the obvious difference of return the values in the context ‘AsOf’ the required argument.  See the example below.

```SQL
	-- select
	select * from [ERPS].[TransactionHeader.AsOf]('2014-12-31 23:33:40.2821262');
```

A simple date will work as well, as seen below.

```SQL
	-- select
	select * from [ERPS].[TransactionHeader.AsOf]('2014-12-31');
```

Columns include: SNK, related attributes.

### {6.5} ‘UpsertQueue' Abstraction:
The ‘UpsertQueue’ abstraction is a temporary staging table used to gather records that have either been inserted or updated in the source system.  This is ideal for ETL processes that utilize data flow tasks which pump records from one database to another.  Once data is loaded in this abstraction, the ‘Process Method’ can be called to load the data into the actual entity object.

```SQL
	insert [ERPS].[TransactionHeader.Upsert] ([TransactionNumber],[Amount],[Date]) 
	values (1,2,'2013-12-31');
```

Columns include: SNK, related attributes.
File Group: TMP

###{6.6} ‘DeleteQueue’ Abstraction:
The ‘DeleteQueue’ abstraction is another temporary staging table used to isolate records that have been deleted from the source system.  Again, this is ideal for ETL processes that utilize data flow tasks which pump records from one database to another.  Once the SNK has been loaded in this abstraction, the ‘Process Method’ can be called to mark the entity object as deleted. 

```SQL
	insert [ERPS].[TransactionHeader.Delete] ([TransactionNumber]) 
	values (3);
```

Columns include: SNK.
File Group: TMP

### {6.7} ‘ProcessLoad’ Method:
The ‘ProcessLoad’ method consumes data from both the ‘UpsertQueue’ and ‘DeleteQueue’ abstractions.  The method is a database stored procedure which ensures data is persisted to the appropriate entities.  Should an error occur with the stored procedure, the ‘Return Value’ will return the appropriate error code.   An example is below:

```SQL
	declare @rv int;
	exec @rv=[ERPS].[TransactionHeader.ProcessLoad];
	print @rv;
```

### {6.8} ‘Archive’ Abstraction:
The ‘Archive’ abstraction is a non-temporal database view which contains all the SNK’s that have been moved to the archive partition.  It is important to note that any given SNK is made up of the sources’ unique business key and a change sequence.  Archiving records entails archiving this entire set, not just single temporal records.  This abstraction allows for the viewing of these archived sets.  Additionally, DELETE statements are allowed on this abstraction; however, once committed, this is a permanent delete and cannot be restored without a backup.

```SQL
	-- select
	select * from [ERPS].[TransactionHeader.Archive];
	-- delete
	delete [ERPS].[TransactionHeader.Archive];
```

Columns include: SNK, related attributes.
File Group: ARCHIVE

### {6.9}	‘ArchiveQueue’ Abstraction:
Similar to the ‘UpsertQueue’ and ‘DeleteQueue’, the ‘ArchiveQueue’ holds the SNK for any given record.  It is a temporary table staging table, and once records are placed in it, the ‘ProcessArchive’ method may be called to mark the SNK as archived and move it to the appropriate partition.

```SQL
	insert [ERPS].[TransactionHeader.ArchiveQueue] ([TransactionNumber]) 
	values (5);
```

Columns include: SNK.
File Group: TMP

### {6.10} ‘ProcessArchive’ Method:
The ‘ProcessArchive’ method consumes data from the ‘ArchiveQueue’ abstraction.  The method is a database stored procedure which ensures data full SNK sets are transitioned to the appropriate partition.  Should an error occur with the stored procedure, the ‘Return Value’ will return the appropriate error code.  An example is below.

```SQL
	declare @rv int;
	exec @rv=[ERPS].[TransactionHeader.ProcessArchive];
	print @rv;
```

### {6.11} ‘Changes’ Abstraction:
The ‘Change’ abstraction allows for downstream data repositories to pull record sets from the PSA for a specific version of each entity.  The ‘version’ is tracked as a ‘bigint’, begins at one (1) and increases by one (1) for each DML statement executed within the PSA database – this is systemic and cannot be altered.  The abstraction itself is a Table-Valued function that accepts the ‘version’ as an argument and returns the relevant result based on any changes made since that incrementation was generated.  A ‘System Object’ has been defined (psa_change_tracking_entity_version) to capture which versions have been pulled successfully.  You can access the changes with the below logic.

```SQL
	-- step one: get usable version number
	declare @x int
	exec @x=[dbo].[psa_get_change_tracking_entity_version] N'[ERPS].[TransactionHeader]';
	
	-- step two: pull changes
	select * from [ERPS].[TransactionHeader.Changes](@x);
	
	-- step three: log version number as complete
	exec [dbo].[psa_set_change_tracking_entity_version] N'[ERPS].[TransactionHeader]';
```

Columns include: SNK, related attributes.

### {6.12}	Purging the Archive:
Purging data completely out of the PSA is relatively easy; however, the governance, stewardship, and process around how it is done needs to defined by the business itself and ultimately is architecture irrelevant and will not be explicitly called out in this particular document.  When appropriate, a data purge can be conducted through a DELETE statement on the ‘Archive’ abstraction.  An example is below.

```SQL
	-- purge data from psa
	delete
	   [ERPS].[TransactionHeader.Archive]
	where
	   [TransactionNumber]=1;
```

### {6.13} Abstraction & Method Flow Visual:

![alt text](https://github.com/mjfii/Persistent-Staging-Area/raw/master/images/psa_fig_2.png "PSA Control Abstract Data Flow")
 
## {7}	Security:
PSA security will be maintained through database roles and schemas, and ultimately, active directory groups that may be assigned to each of the roles.

### {7.1}	Schemas:
As stated above, each source system is assigned a schema, and each schema is owned by the [psa_owner] database role.  All entities and abstractions are assigned to a source system schema, and an ownership (from the bottom up) example is seen below.

![alt text](https://github.com/mjfii/Persistent-Staging-Area/raw/master/images/psa_fig_3.png "PSA Schema Ownership")
 
### {7.2}	Database Roles:
Each abstraction and method type will be accessible by one or more database role, equating roughly to a functional role, whether an actual user or some sort of service account.   The allowable security can be seen below by database role by abstraction/method.  Note: since all entities are owned by [psa_owner] role (via schemas), the following roles will not have access to the entity object itself.  Access to these underlying objects is only granted to users in the [psa_owner] role, or, of course, [db_owner].

![alt text](https://github.com/mjfii/Persistent-Staging-Area/raw/master/images/psa_fig_4.png "PSA Abstract Ownership")

### {7.3}	Application Roles:
Since this solution is used primarily for Online Analytical Processing (“OLAP”), and not Online Transaction Processing (“OLTP”), i.e. the business users will not be modifying the data within these systems, there is little need to consider application roles.  Since directly accessing the PSA will add little business value, it is recommended that it only be utilized as a source to build new analytical requirements and/or conduct requirements research.

### {7.4}	Users:
It is not recommended that individual users have credentials to access the PSA directly.  However, it is important to plan for this requirement should the need arise.  As noted above there are five (5) defined Database Roles with very specific securitization across all data access needs.  Active Directory Users or User Groups should be applied directly to one (1) or more of these database roles.

## {8}	Data Extraction Logic:
While it is recommended to interface with the PSA data through the provided abstractions, it is understood that this may not be feasible for all occasions.  In this case, the following methodologies can be used to extract information per any business requirements.  The below logic contains the general logic (simple predicates) maintained in the previously covered abstractions.

### {8.1}	Determine Current Records with Deletions:
```SQL
	where [psa_end_period] is null;
```

### {8.2} Determine Current Records without Deletions:

```SQL
	where [psa_end_period] is null and [psa_active_state]=1;
```

### {8.3}	Determine Deleted Records:

```SQL
	where [psa_end_period] is null and [psa_active_state]=0;
```

### {8.4}	Determine Entity Value at a Point in Time:
```SQL
	where
	   [psa_active_state]=1
	   and
	   [psa_start_period]<=@AsOfDateTime
	   and
	   isnull([psa_end_period],'9999-12-31')>=@AsOfDateTime;
```

### {8.5}	Determine DML Actions Conducted, i.e. INSERTS:

```SQL
	where [psa_dml_action]=N'I'; -- I=INSERT,U=UPDATE,D=DELETE
```

### {8.6}	Determine Archived Records:

```SQL
	where [psa_archive_flag]=1;
```

### {8.7}	Determine Changes From a Given Batch:

```SQL
	where [psa_batch_id]='351DC14C-412F-4C13-8EF2-03C08D232AC3';
```

8/6/2014 3:36:45 PM 