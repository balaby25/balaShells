#!/usr/bin/perl

use Sys::Hostname;
use File::Copy;

$numArgs = $#ARGV + 1;

if ($numArgs != 1) {
 print "usage: $0 your_awrreport_identifier    \n ";
 exit;
}

$report_name_pl_arg = $ARGV[0];


$ohome="/u01/app/oracle/product/12.2.0/dbhome_1";
$report_name_pl="/tmp/awr_$report_name_pl_arg.html" ;
$pass_num_days_pl=1 ;

$ENV{"ORACLE_HOME"}=$ohome;
$ENV{"LD_LIBRARY_PATH"}="$ohome/lib";


sub grant_privs{
 open ORA, "| $ohome/bin/sqlplus -s / as sysdba  " or die "Can't pipe to sqlplus: $!";

 print ORA "GRANT EXECUTE ON DBMS_MONITOR to PERFTEST;  \n";
 print ORA "GRANT ALTER SESSION TO  PERFTEST;  \n";
 print ORA "GRANT CONNECT, RESOURCE, ADVISOR TO PERFTEST; \n";
 print ORA "GRANT SELECT ANY DICTIONARY TO PERFTEST; \n";
 print ORA "GRANT EXECUTE ON DBMS_WORKLOAD_REPOSITORY TO PERFTEST; \n";
 print ORA "exit\n";
 close ORA;
}

sub trace10046{
 open ORA, "| $ohome/bin/sqlplus -s perftest/perftest  " or die "Can't pipe to sqlplus: $!";

 #print ORA "column  pass_inst_name new_value inst_name noprint ;\n";
 #print ORA "select instance_name as pass_inst_name from v\$instance ;\n";
 print ORA "set  feedback off ; \n";
 print ORA "set  pagesize 0 ; \n";
 print ORA "ALTER SESSION SET TRACEFILE_IDENTIFIER = \"db_io_test\" ; \n";
 print ORA "ALTER SESSION SET sql_trace=TRUE; \n";
 print ORA "ALTER SESSION SET EVENTS '10046 trace name context forever, level 8'; \n";
 print ORA "EXEC DBMS_MONITOR.session_trace_enable; \n";
 print ORA "select count(*) from testtable ; \n";
 
 print ORA "exit\n";
 close ORA;
}

sub exec_sql{
 open ORA, "| $ohome/bin/sqlplus -s perftest/perftest  " or die "Can't pipe to sqlplus: $!";

 print ORA "column pass_begin_snap new_value begin_snap noprint ;\n";
 print ORA "select dbms_workload_repository.create_snapshot() as pass_begin_snap from dual ; \n";

 print ORA "set  feedback off ; \n";
 print ORA "set  pagesize 0 ; \n";
 print ORA "set autotrace on ; \n";
 print ORA "set  trimspool on ; \n";
 print ORA "spool /tmp/ins2.txt ; \n";
 print ORA "alter session set parallel_degree_policy=manual ; \n";
 #print ORA "alter table testtable noparallel ; \n";
 #print ORA "select /*+ no_parallel (t) */ count(*) from testtable partition(P1) t; \n";
 print ORA "alter table testtable parallel ; \n";
 print ORA "execute ins_into_testtable(3, 4000000) ; \n";

 print ORA "column pass_end_snap new_value end_snap noprint ;\n";
 print ORA "select dbms_workload_repository.create_snapshot() as pass_end_snap from dual ; \n";

 print ORA "define  report_type  = 'html';\n";
 print ORA "define  report_name  = '$report_name_pl' ;\n";
 
 print ORA "column  pass_inst_num new_value inst_num noprint ;\n";
 print ORA "select instance_number as pass_inst_num from v\$instance  ;\n";

 print ORA "column  pass_num_days new_value num_days noprint ;\n";
 print ORA "select $pass_num_days_pl as pass_num_days from dual; \n";
 
 print ORA "column  pass_inst_name new_value inst_name noprint ;\n";
 print ORA "select instance_name as pass_inst_name from v\$instance ;\n";
 
 print ORA "column  pass_db_name new_value db_name noprint ;\n";
 print ORA "select name as pass_db_name from v\$database ;\n";
 
 print ORA "column  pass_dbid new_value  dbid noprint ;\n";
 print ORA "select dbid as pass_dbid   from v\$database ;\n";
 
 print ORA "\@$ohome/rdbms/admin/awrrpti; \n";
 print ORA "exit\n";
 close ORA;
 copy("$report_name_pl","/net/slc09stm/scratch/balakcha/lframe/stats/db_io") or die "Copy failed: $!";
 print "\n Have fun. Your awr report is at :  \n http://slc09stm.us.oracle.com/scratch_balakcha_lframe_stats/db_io/awr_$report_name_pl_arg.html \n\n";
}
# gen_awr_report;
#  grant_privs;
  exec_sql;
