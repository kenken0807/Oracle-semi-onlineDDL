#!/usr/local/bin/perl
use strict;
use utf8;
use DBD::Oracle qw(:ora_types);
use Getopt::Long;
use Data::Dumper;
#--db      SID
#--password password
#--user  username
#--host hostname[localhost]
#--port listener port [1521]
#--table tablename

my @IDX_CREATES;
my @IDX_DROPS;
my @ALTER_TBL;
my $RENAME_IDX="--TABLENAME--";
my %opts=(host=>"localhost",port=>"1521");
GetOptions(\%opts,'db=s',
                  'password=s',
                  'user=s',
                  'host=s',
                  'port=s',
                  'help',
                  'disable-foreignkey',
                  'table=s',
                  'dryrun',
                  'execute',
                  'idx=s'=>\@IDX_CREATES,
                  'rebuild',
                  'dropidx=s'=>\@IDX_DROPS,
                  'auto',
                  'alter=s'=>\@ALTER_TBL);
#check option
if(!$opts{db} || !$opts{password} || !$opts{user} || !$opts{table}){
  &Print_Out( "[ERROR] Check Options");
  &HelpSTD;
  exit;
}

if(($opts{dryrun} && $opts{execute}) || (!$opts{dryrun} && !$opts{execute}))
{
  &Print_Out( "[ERROR] Choose One --dryrun or --execute");
  &HelpSTD;
  exit;
}

if(! @IDX_CREATES && ! $opts{rebuild} && ! @IDX_DROPS && ! @ALTER_TBL)
{
   &Print_Out( "[ERROR] Choose --idx or --rebuild or --dropidx or --alter");
   &HelpSTD;
   exit;
}

if($opts{help})
{
  &HelpSTD;
  exit;
}


my $DRY_RUN;
$DRY_RUN =1 if($opts{dryrun});
$DRY_RUN =0 if($opts{execute});
my $FOREIGN_KEY_DISABLE=0;
$FOREIGN_KEY_DISABLE=1 if($opts{"disable-foreignkey"});

#connect DB
my $DB_CONF =
    {host=>  $opts{host},
     port=>  $opts{port},
     db_name=>  $opts{db},
     db_user=>  $opts{user},
     db_pass=>  $opts{password},
    };

my $DBH = Connect_db();

$DBH->{LongReadLen} = 1024 * 200;
$DBH->{AutoCommit} =0;
my $STH = $DBH->do("ALTER SESSION SET ddl_lock_timeout = 5") || die DBI->errstr."$!";
my $USER= uc $opts{user};
my $TABLENAME = uc $opts{table};
my $DROPIDXS;
my $AUTO=$opts{auto};
my $NEWDROPIDX="";
my $NEWTABLE="NEW__".$TABLENAME;
my $OLDTABLE="OLD__".$TABLENAME;
my $MVIEW="MV__".$TABLENAME;
my $TRGNAME="TRG__".$TABLENAME;
my $MVIEWLOG="";
my $INDEXDDLS;
my ($TRUE,$FALSE)=(1,0);

eval {
  Main();
};
if($@){
  &Print_Out("\n\n\n[ERROR]Exception Error");
  eval {
    &Dryrun_Drop_Objects();
  };
  if($@){
    &Print_Error();
  }
}

#main
sub Main {
  ###########
  #check objects
  ############
  #check new index
  if(@IDX_CREATES)
  {
    foreach my $cidx (@IDX_CREATES)
    {
      if( $cidx !~ /$RENAME_IDX/)
      {
        &Print_Out("[ERROR]--idx tablename must be \"$RENAME_IDX\"");
        exit;
      }
    }
  }
  #check drop obj
  if(@IDX_DROPS)
  {
    foreach my $didx (@IDX_DROPS)
    {
      $DROPIDXS->{uc $didx}=$TRUE;
    }
  }
  #check objects
  my $getnmsql="select substr(?,0,30),substr(?,0,30),substr(?,0,30),substr(?,0,30) from dual";
  my $sth = $DBH->prepare($getnmsql) || die DBI->errstr."$!";
  $sth->execute($NEWTABLE,$OLDTABLE,$MVIEW,$TRGNAME) || die DBI->errstr."$!";
  ($NEWTABLE,$OLDTABLE,$MVIEW,$TRGNAME)=$sth->fetchrow();
  $sth->finish();
  &Print_Out("Tablename($TABLENAME) NewTablename($NEWTABLE) OldTablename($OLDTABLE) Mvname($MVIEW) Triggername($TRGNAME)");

  my $chsql="select OBJECT_NAME from user_objects where OBJECT_NAME in (?,?,?,?,?)";
  $sth = $DBH->prepare($chsql) || die DBI->errstr."$!";
  $sth->execute($TABLENAME,$NEWTABLE,$MVIEW,$OLDTABLE,$TRGNAME) || die DBI->errstr."$!";
  my $chflg=0;
  while (my $tnm = $sth->fetchrow())
  {
    if($tnm eq $TABLENAME)
    {
      $chflg=1;
    }
    if($tnm eq $NEWTABLE)
    {
      &Print_Out("[ERROR]Already Exists ($NEWTABLE)");
      exit;
    }
    if($tnm eq $MVIEW)
    {
      &Print_Out( "[ERROR]Already Exists ($MVIEW)");
      exit;
    }
    if($tnm eq $OLDTABLE)
    {
      &Print_Out("[ERROR]Already Exists ($OLDTABLE)");
      exit;
    }
  }
  if($chflg==0)
  {
    &Print_Out("NOT Found Table($TABLENAME)");
    exit;
  }
  $sth->finish();

  #check table constraints
  my ($consts,$consttypes) = &Chk_Constraint($TABLENAME);
  #check indexex(except foreign key and unique)
  &Check_Indexes();
  
  #Get Table ddl
  my $tableddlsql = &Get_DDL("TABLE",$TABLENAME,$NEWTABLE,$consts,$consttypes);
  #Create NewTable
  &Create_NewTable($tableddlsql);

  #alter table at new table
  &Alter_NewTable($NEWTABLE,@ALTER_TBL) if(@ALTER_TBL);
  
  #create indexex
  &Create_Indexes();  

  #create mview log
  &Create_MviewLog();  
  
  #create mv
  my ($newcolpk,@newcols)=&GetTableCols($TABLENAME,$NEWTABLE);
  my $newcols=join(',',@newcols);
  &Create_Mview($newcols);

  #copy data from Mview to New table
  &CopyDataMV2Newtable($newcols);

  #create trigger  
  &Create_Trigger($newcols,$newcolpk,@newcols);

  #if dry-run ,finish the script
  if($DRY_RUN)
  {
    &Dryrun_Drop_Objects();
    &Print_Out( "[DRY-RUN] complete, Does not alter");
    exit;
  }

  ############
  #refresh mview
  ############
  &Refresh_Mview();  
  my $mlogcnt;
  #if --auto exists, no pause
  if($AUTO)
  {
    while(1){
      &Refresh_Mview();
      if(&Chk_Mviewlog < 50)
      {
        &Print_Out( "Mview log Cnt < 10. Quit loop");
        last;
      }
    }
  #if --auto does not exist,pause
  }else{
    while(1)  {
      my $str;
      print "\n=============================================\n";
      print "Choose Number and Enter key\n";
      print "1 or Only EnterKey: Refresh Mview($MVIEW)\n";
      print "2: Rename Table($TABLENAME)\n";
      print "9: Cancel\n";
      print "=============================================\n";
      chomp($str = <STDIN>);
      if($str eq 1){
        $mlogcnt=&Chk_Mviewlog;
        &Print_Out( "Mview log cnt: $mlogcnt");
        &Refresh_Mview();
        next;
      }
      elsif($str eq 2)
      {
        last;
      }
      elsif($str eq 9)
      {
        &Print_Out( ">>>>>Cancel Alter table");
        &Dryrun_Drop_Objects();
        exit;
      }
      else
      {
        $mlogcnt=&Chk_Mviewlog;
        &Print_Out( "Mview log cnt: $mlogcnt");
        &Refresh_Mview();
        next;
      }
    }
  }
  &Refresh_Mview();
  #############
  #switch table to  newTable
  #############
  #OLD table readonly
  my $sth = $DBH->do("ALTER SESSION SET ddl_lock_timeout = 5") || die DBI->errstr."$!";
  eval {
    &Print_Out(  ">>>Read Only Table ($TABLENAME)");
    $sth=$DBH->prepare("ALTER TABLE $TABLENAME READ ONLY") || die DBI->errstr."$!";
    $sth->execute() || die DBI->errstr."$!";
    $sth->finish();
  };
  if($@){
    &Dryrun_Drop_Objects();
    &Print_Error();
    exit;
  }
  eval{
    #refresh mv
    &Refresh_Mview();
    
    #drop mview
    &Print_Out(  "Drop Mview log($TABLENAME)");
    $sth = $DBH->do("DROP MATERIALIZED VIEW LOG ON $TABLENAME") || die DBI->errstr."$!";
    
    #rename table oldtable
    &Print_Out(  "Rename Table ($TABLENAME to $OLDTABLE)");
    $sth = $DBH->do("ALTER TABLE $TABLENAME  RENAME TO $OLDTABLE") || die DBI->errstr."$!";
  };
  
  if($@){
    &Print_Out( "\n>>>Change Read Write Table($TABLENAME)");
    my $sth=$DBH->prepare("ALTER TABLE $TABLENAME READ WRITE") || die DBI->errstr."$!";
    $sth->execute() || die DBI->errstr."$!";
    $sth->finish();
    &Print_Error();
    exit;
  }
  #NewTable rename
  eval {
    &Print_Out( "Rename Table ($NEWTABLE to $TABLENAME)");
    $sth = $DBH->do("ALTER TABLE $NEWTABLE  RENAME TO $TABLENAME") || die DBI->errstr."$!";
  };
  
  if($@){
    &Print_Out( "Rename Table ( $OLDTABLE to $TABLENAME)");
    my $sth = $DBH->do("ALTER TABLE $OLDTABLE RENAME TO $TABLENAME ") || die DBI->errstr."$!";
    &Print_Out( "\n>>>Change Read Write Table($TABLENAME)");
    my $sth=$DBH->prepare("ALTER TABLE $TABLENAME READ WRITE") || die DBI->errstr."$!";
    $sth->execute() || die DBI->errstr."$!";
    $sth->finish();
    &Print_Error;
    exit;
  }
  eval {
    &Print_Out( ">>>Finish ReadOnly");
    &Drop_Objects();
  };
  
  if($@){
    &Print_Out("already rename $NEWTABLE to $TABLENAME");
    &Print_Out("[ERROR]failed drop some objects")
  }
  &Print_Out("Finish Completely");
  &Print_Out("You need to rename CONSTRAINT_NAME and INDEX_NAME");
}

##################
#On Off Foreign Key
##################
sub OnOff_FK {
  my $type=shift;
  my $table=shift;
  my $consts=shift;
  my $sth;
  foreach my $constname (keys(%$consts))
  {
    next if($consts->{$constname} ne "R");
    &Print_Out(  "$type $constname ($table)");
    $sth = $DBH->do("ALTER TABLE $table $type CONSTRAINT $constname")  || die DBI->errstr."$!";
  }
}

############
#Alter table at Newtable
############
sub Alter_NewTable {
  my $table=shift;
  my @alters=@_;
  my $sth;
  foreach my $alter (@alters)
  {
    $alter= uc($alter);
    if($alter=~/RENAME\s/)
    {
      &Print_Out("[ERROR] RENAME clause is not corresponding");
      &Dryrun_Drop_Objects();
      exit;
    }
    my $sql="ALTER TABLE $table $alter";
    &Debug_Print($sql);
    &Print_Out("ALTER ($table) ($sql)");
    $sth = $DBH->do($sql) || die DBI->errstr."$!";
  }
}

############
#check indexex(except foreign key and unique)
############
sub Check_Indexes {
   my $idxsql=<<EOF;
select
  a.INDEX_NAME
  ,substr('NEW__' || a.INDEX_NAME,0,30)
  ,substr('OLD__' || a.INDEX_NAME,0,30)
  ,max(c.INDEX_NAME)
from user_ind_columns a
left join USER_IND_EXPRESSIONS c
on a.INDEX_NAME=c.INDEX_NAME
where
  a.table_name = ?
  and not exists ( select 1 from USER_CONSTRAINTS b where a.INDEX_NAME=b.CONSTRAINT_NAME and b.CONSTRAINT_TYPE in ('P','U'))
group by a.INDEX_NAME
EOF
  my $sth = $DBH->prepare($idxsql) || die DBI->errstr."$!";
  $sth->execute($TABLENAME) || die DBI->errstr."$!";
  while( my ($idx,$newidx,$oldidx,$funcidx) = $sth->fetchrow())
  {
    if($DROPIDXS)
    {
      if($DROPIDXS->{$idx} == $TRUE)
      {
        &Print_Out( ">>>>>Drop Index ($idx)");
        $DROPIDXS->{$idx}=$FALSE;
        next;
      }
    }
    #if function index is exists
    if($funcidx)
    {
      &Print_Out( "Function Index Exists($funcidx)");
#      next;
    }
    my $sth2 = $DBH->prepare("select count(*) from user_indexes where index_name in (?,?)") || die DBI->errstr."$!";
    $sth2->execute($newidx,$oldidx) || die DBI->errstr."$!";
    my $cnt=$sth2->fetchrow();
    if($cnt != 0)
    {
      &Print_Out( "[ERROR]Already Exists ($newidx or $oldidx)");
      exit;
    }
    $sth2->finish();
    $INDEXDDLS->{$newidx}=&Get_DDL("INDEX",$idx,$newidx,"","");
    &Print_Out( "idx($idx) newidx($newidx) oldidx($oldidx)");
  }
  $sth->finish();
  #if --dropidx is exists and it doesnot match index is exists and dropindex
  if($DROPIDXS)
  {
    foreach my $didx (keys(%$DROPIDXS))
    {
      if($DROPIDXS->{$didx})
      {
        &Print_Out( "[ERROR] $TABLENAME does not have INDEX $didx");
        exit;
      }
    }
  }
}

############
#Get Table ddl
############
sub Get_DDL {
  my $type=shift;
  my $obj=shift;
  my $newobj=shift;
  my $consts=shift;
  my $consttypes=shift;
  my $pretty=qq{
                 BEGIN
                    DBMS_METADATA.SET_TRANSFORM_PARAM( DBMS_METADATA.SESSION_TRANSFORM, 'PRETTY', TRUE); 
                    DBMS_METADATA.SET_TRANSFORM_PARAM( DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE',FALSE);
                    DBMS_METADATA.SET_TRANSFORM_PARAM( DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES',  TRUE);
                 END;
               };
  
  my $sql="select dbms_metadata.get_ddl(?,?) as DL from dual";
  my $sth;
  $sth = $DBH->do($pretty) || die DBI->errstr."$!";
  $sth = $DBH->prepare($sql) || die DBI->errstr."$!";
  $sth->execute($type,$obj) || die DBI->errstr."$!";
  my $ddlsql = $sth->fetchrow();
  if($type eq "TABLE")
  {
    $ddlsql=~s/CREATE TABLE "$USER"."$obj"/CREATE TABLE "$USER"."$newobj"/g;
    foreach my $cname(keys(%$consts))
    {
      # rename constraint name
      $ddlsql=~s/CONSTRAINT "$cname"/CONSTRAINT "$consts->{$cname}"/g;
      # change foreign key disable
      if($consttypes->{$consts->{$cname}} eq "R")
      {
        &Print_Out("DIABLE FOREIGN KEY($consts->{$cname})");
        $ddlsql=~s/(REFERENCES\s.+\s.+\s)ENABLE/$1DISABLE/g;
      } 
    }
  }
  if($type eq "INDEX")
  {
    $ddlsql=~s/INDEX "$USER"."$obj" ON "$USER"."$TABLENAME"/INDEX "$USER"."$newobj" ON "$USER"."$NEWTABLE"/g;
  }
  &Debug_Print($ddlsql) ;
  $sth->finish();
  return $ddlsql;
}

############
#create mview log
############
sub Create_MviewLog {
  my $mvlogsql="create materialized  view log on $TABLENAME NOLOGGING";
  my $mvlognmsql="select LOG_TABLE from all_mview_logs where MASTER=?";
  &Print_Out( "Create MVLOG ($TABLENAME)");
  my $sth = $DBH->do($mvlogsql) || die DBI->errstr."$!";
  $sth = $DBH->prepare($mvlognmsql) || die DBI->errstr."$!";
  $sth->execute($TABLENAME) || die DBI->errstr."$!";
  $MVIEWLOG=$sth->fetchrow();
  $sth->finish();
  &Debug_Print( "$mvlogsql") ;
  if($DRY_RUN){
    &Print_Out( "[DRY-RUN] Drop MVLOG($TABLENAME)");
    $sth = $DBH->do("drop materialized  view log on $TABLENAME") || die DBI->errstr."$!";
  }
}

############
#create mv
############
sub Create_Mview {
  my $mvsql;
  my $cols=shift;
  if($DRY_RUN)
  {
    $mvsql="create materialized view $MVIEW NOLOGGING  as select $cols from $TABLENAME where rownum < 2 ";
  }else{
       $mvsql="create materialized view $MVIEW nologging refresh fast as select $cols from $TABLENAME ";
  }
  &Print_Out( "Create Mview ($MVIEW)");
  &Debug_Print($mvsql) ;
  my $sth = $DBH->do($mvsql) || die DBI->errstr."$!";
}
############
#create newtable
############
sub Create_NewTable {
  my $tableddlsql=shift;
  &Print_Out( "Create Table ($NEWTABLE)");
  my $sth= $DBH->do($tableddlsql) || die DBI->errstr."$!";
}

############
#Get Table Columns
############
sub GetTableCols {
  my ($tablename,$new_table)=(shift,shift);
  my (@cols,$colpk);
  my $sql=qq{
            select a.COLUMN_NAME,b.COLUMN_NAME,i.COLUMN_NAME,a.COLUMN_ID,b.COLUMN_ID from
            (select COLUMN_NAME,COLUMN_ID from user_tab_cols where TABLE_NAME=? and HIDDEN_COLUMN='NO' and VIRTUAL_COLUMN='NO') a 
            full outer join 
            (select COLUMN_NAME,COLUMN_ID from user_tab_cols where TABLE_NAME=? and HIDDEN_COLUMN='NO' and VIRTUAL_COLUMN='NO') b 
            on a.COLUMN_NAME=b.COLUMN_NAME
            left join 
            (select COLUMN_NAME from user_ind_columns uic join user_constraints uc on uic.INDEX_NAME=uc.CONSTRAINT_NAME where uc.CONSTRAINT_TYPE='P' and uc.TABLE_NAME=? ) i 
            on b.COLUMN_NAME=i.COLUMN_NAME
            order by a.COLUMN_ID 
            };
  my $sth = $DBH->prepare($sql) || die DBI->errstr."$!";
  $sth->execute($tablename,$new_table,$new_table) || die DBI->errstr."$!";
  while (my ($tcol,$ncol,$pk,$tn,$nn)=$sth->fetchrow())
  {
    if($tcol eq $ncol)
    {
      push(@cols,$ncol);
      if($pk)
      {
        $colpk->{$ncol}=1;
      }
      else
      {
        $colpk->{$ncol}=0;
      }
    }
  }
  $sth->finish();
  return ($colpk,@cols);
}

############
#copy data Mview to New table
############
sub CopyDataMV2Newtable {
  my $cols=shift;
  my $copysql="insert /*+ APPEND */ into $NEWTABLE($cols) select $cols from $MVIEW";
  if($DRY_RUN)
  {
    $copysql=$copysql." where rownum < 2";
    &Debug_Print($copysql);
    &Print_Out( "[DRY-RUN]COPY TABLE ($MVIEW to $NEWTABLE) 1 row");
  }
  else
  {
    &Print_Out( "Copy Table ($MVIEW to $NEWTABLE)");
  }
  my $sth = $DBH->do($copysql) || die DBI->errstr."$!";
}

############
#create indexex
############
sub Create_Indexes {
  my $sth;
  foreach my $nidx (keys(%$INDEXDDLS))
  {
    $sth= $DBH->do($INDEXDDLS->{$nidx}) || die DBI->errstr."$!";
#    &Print_Out( "[DEBUG] $INDEXDDLS->{$nidx}") if($DRY_RUN);
    &Print_Out( "Create Index ($nidx)");
  }
  if(@IDX_CREATES)
  {
    foreach my $cidx (@IDX_CREATES)
    {
      $cidx=~s/$RENAME_IDX/$NEWTABLE/g
      &Print_Out( ">>>>>CREATE NEW INDEX($cidx)");
      $sth= $DBH->do($cidx) || die DBI->errstr."$!";
    }
  }
}

############
#Create_Trigger
############
sub Create_Trigger{
  my ($cols,$colpk,@cols)=(shift,shift,@_);
  my $inshead="INSERT INTO $NEWTABLE ($cols) VALUES ( ";
  my $upshead="UPDATE $NEWTABLE SET ";
  my $delhead="DELETE FROM $NEWTABLE ";
  my ($ins,$ups,$del);
  my $where="";
  foreach my $col (keys %$colpk)
  {
    my $pkcol=$colpk->{$col};
    if($pkcol)
    {
      $where= $where ? " AND $col = :OLD.$col " : " WHERE $col = :OLD.$col ";
    }
    $ups= $ups ? "$ups, $col = :NEW.$col "  : $ups." $col = :NEW.$col ";
  }
  foreach my $col(@cols)
  {
    $ins= $ins ? "$ins, :NEW.$col "  : $ins." :NEW.$col ";
  }
  $ins=$inshead.$ins.")";
  $ups=$upshead.$ups.$where;
  $del=$delhead.$del.$where;
  my $trisql=<<EOF;
 create or replace trigger $TRGNAME
 after insert or delete or update on $MVIEW for each row
 begin
  IF (INSERTING) THEN
    $ins;
  END IF;
  IF (UPDATING) THEN
    $ups;
  END IF;
  IF (DELETING) THEN
    $del;
  END IF;
 end;
EOF
  &Debug_Print($trisql) ;
  &Print_Out( "Create Trigger ($TRGNAME)");
  my $sth = $DBH->do($trisql) || die DBI->errstr."$!";
}

############
#chack table Constraint
############
sub Chk_Constraint {
  my $table=shift;
  my $consts;
  my $consttypes;
  my $pknm;
  my $constsql="select CONSTRAINT_NAME,substr('NEW__' || CONSTRAINT_NAME,0,30) as newconstname,CONSTRAINT_TYPE from USER_CONSTRAINTS where TABLE_NAME= ? and CONSTRAINT_TYPE in (?,?,?,?)";
  my $sth = $DBH->prepare($constsql) || die DBI->errstr."$!";
  $sth->execute($table,"P","U","C","R") || die DBI->errstr."$!";
  while ( my ($cname,$newcname,$type) = $sth->fetchrow())
  {
    $pknm=$cname if($type eq "P");
    $consts->{$cname}=$newcname;
    $consttypes->{$newcname}=$type;
    if($type eq "R" && ! $FOREIGN_KEY_DISABLE)
    {
      &Print_Out( "[WARNING]Stop alter table because $table has Foreign Key($cname).");
      &Print_Out( "You need --disable-foreignkey option.");
      &Print_Out( "CAUTION: --disable-foreignkey means disable foreign key at only new table ,not original table. After finish alter table ,the foreign key is still disable.");
      &Print_Out( "Recommend to disable or drop Foreign key($cname) first if alter table");
      exit;
    }
  }
  $sth->finish();
  if(!$pknm)
  {
    &Print_Out( "NOT FOUND Primary key($table)");
    exit;
  }
  #check the target table's PK is used by foreign key
  my $cnt=&ChkFK($pknm);
  if($cnt > 0)
  {
    &Print_Out( "[ERROR]Can't Alter Table($table) because $pknm is used at Foreign Key");
    &Print_Out( "You need drop or disable foreign key is used $pknm ");
    exit;
  }  
  &Print_Out( "PK ($pknm) is not used by Foreign Key");
  return $consts,$consttypes;
}
###########
#check foreign key
###########
sub ChkFK {
  my $pk=shift;
  my $sth=$DBH->prepare("select count(*) from user_constraints where R_CONSTRAINT_NAME = ?") || die DBI->errstr."$!";
  $sth->execute($pk) || die DBI->errstr."$!";
  my $cnt=$sth->fetchrow();
  $sth->finish();
  return $cnt if($cnt == 0);
  $sth=$DBH->prepare("select status from user_constraints where R_CONSTRAINT_NAME = ?") || die DBI->errstr."$!";
  $sth->execute($pk) || die DBI->errstr."$!";
  while (my $status = $sth->fetchrow())
  {
    return $cnt if($status ne "DISABLED");
  }
  $sth->finish();
  return 0;
}

############
#Drop
############
sub Drop_Objects {
  my $sth;
  &Com_Drop_Objects();
  if(&Drop_Object_Chk($MVIEW))
  {
    $sth = $DBH->do("drop materialized view $MVIEW") || die DBI->errstr."$!";
    &Print_Out( "Drop Mview ($MVIEW)");
  }
  if( &Drop_Object_Chk($OLDTABLE))
  {
    $sth = $DBH->do("drop table $OLDTABLE") || die DBI->errstr."$!"; 
    &Print_Out( "Drop Table ($OLDTABLE)");
  }
}

############
#Drop Dryrun
############
sub Dryrun_Drop_Objects {
   my $sth;
  &Com_Drop_Objects();
  if(&Drop_Object_Chk($MVIEWLOG))
  {
    $sth = $DBH->do("drop materialized view log on $TABLENAME") || die DBI->errstr."$!";
    &Print_Out( "Drop Mview Log($TABLENAME)");
  }
   if(&Drop_Object_Chk($NEWTABLE))
   {
      $sth = $DBH->do("drop table $NEWTABLE ") || die DBI->errstr."$!";
      &Print_Out( "Drop Table $NEWTABLE");
   }
}

############
#Com_Drop
############
sub Com_Drop_Objects {
   my $sth;
   if(&Drop_Object_Chk($TRGNAME))
   {
      $sth = $DBH->do("drop trigger $TRGNAME") || die DBI->errstr."$!";
      &Print_Out( "Drop Trigger ($TRGNAME)");
   }
   if(&Drop_Object_Chk($MVIEW))
   {
      $sth = $DBH->do("drop materialized view $MVIEW") || die DBI->errstr."$!";
      &Print_Out( "Drop Mview ($MVIEW)");
   }
}

############
#Drop_Object_Chk
############
sub Drop_Object_Chk {
  my $obj_nm=shift;
  my $objsql="select count(object_name) from user_objects where object_name =?";
  my $sth = $DBH->prepare($objsql) || die DBI->errstr."$!";
  $sth->execute($obj_nm);
  my $cnt = $sth->fetchrow();
  $sth->finish();
  return $cnt;
}

###########
#check Mviewlog
###########
sub Chk_Mviewlog {
  my $cntmvlogsql="select count(*) from $MVIEWLOG";
  my $sth = $DBH->prepare($cntmvlogsql) || die DBI->errstr."$!";
  $sth->execute() || die DBI->errstr."$!";
  my $cnt= $sth->fetchrow();
  $sth->finish();
  return $cnt;
}

############
#refresh mview
############
sub Refresh_Mview {
  &Print_Out( "Fast Refresh Start ($MVIEW)");
  my $refsql="begin dbms_mview.refresh('$MVIEW','f'); end;";
  my $sth = $DBH->prepare($refsql) || die DBI->errstr."$!";
  $sth->execute() || die DBI->errstr."$!";
  $sth->finish();
  &Print_Out( "  End");
}

############
#oracle connection
############
sub Connect_db {

    my $db = join(';',"dbi:Oracle:host=$DB_CONF->{host}","sid=$DB_CONF->{db_name}","port=$DB_CONF->{port}");
    my $db_uid_passwd = "$DB_CONF->{db_user}/$DB_CONF->{db_pass}";
    my $DBH = DBI->connect($db, $db_uid_passwd, "");
    return $DBH;
}

############
#print Error
###########
sub Print_Error {
  &Print_Out( "DROP TABLE $NEWTABLE; if it exists");
  &Print_Out( "DROP MATERIALIZED VIEW LOG ON $TABLENAME; if it exists");
  &Print_Out( "DROP MATERIALIZED VIEW  $MVIEW; if it exists");
  &Print_Out( "DROP TRIGGER $TRGNAME; if it exists");
}

############
#option help
############
sub HelpSTD{
  print << "EOS"
  Usage: perl $0 [options] 
       
    --dryrun Create and alter the new table, but do not copy table[default none(MUST choose --dryrun or --execute)]
    --execute Create and alter the new table and DO copy table[default none(MUST choose --dryrun or --execute)]
        
  Connect OPTIONS:
    --db       SID[dafault none]
    --user     username[default none]
    --password user's password[default none]
    --host     hostname or IP[default localhost]
    --port     listener port[default 1521]
    --table    tablename [dafault none]

  CREATE NEW INDEX:
    --idx [create index statement] the tablename should be "$RENAME_IDX"  
          ex.   --idx "create index TEXT_IDX on $RENAME_IDX (col1,col2) tablespace test_ts"
          ex.   --idx "create index TEXT_IDX2 on $RENAME_IDX (col2 desc) tablespace test_ts"
  
  DROP INDEX:
    --dropidx [INDEX NAME] Drop Index
               ex. --dropidx "IDX_TEST"
  
  REBUILD INDEX:
    --rebuild REcreate Table and Indexes
  
  ALTER TABLE:
    --alter [alter statement after ALTER TABLE XXXX] 
             ex. --alter "drop (text_col)" 
             ex. --alter "modify id varchar2(10) not null"
  
  OTHERS:
    --auto Rename Table Auto[default none]
    --disable-foreignkey  The Option is to disable the foreign key that has the target table.After finish to alter table,the foreign key is still disable.


EOS
}
###########
#print
###########
sub Print_Out {
  my $str=shift;
  print &GetTime.$str."\n";
}

############
#Get Time
###########
sub GetTime {
  my @time = localtime;
  return sprintf("%02d:%02d:%02d   :",$time[2],$time[1],$time[0]);
}

############
#DEBUG PRINT
###########
sub Debug_Print{
  return if(! $DRY_RUN);
  my $out=shift;
  print( "\n----------------------------------------------------------\n");
  print( "[DEBUG] \n");
  print( "$out");
  print( "\n----------------------------------------------------------\n");
}
