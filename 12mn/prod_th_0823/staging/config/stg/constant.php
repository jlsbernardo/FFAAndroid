<?php
$jktHostName = 'ffasqldbqa-impvprj.mysql.database.azure.com';
$jktUserName = 'ffasqldbqaimp@ffasqldbqa-impvprj';
$jktPassword = '9J$1#Q-(XjUNJe';
$jktDatabase = 'ffa_asean_jkt_test';

$myHostName = 'ffasqldbqa-impvprj.mysql.database.azure.com';
$myUserName = 'ffasqldbqaimp@ffasqldbqa-impvprj';
$myPassword = '9J$1#Q-(XjUNJe';
$myDatabase = 'ffa_asean_mys_test';

$phpHostName = 'ffasqldbqa-impvprj.mysql.database.azure.com';
$phpUserName = 'ffasqldbqaimp@ffasqldbqa-impvprj';
$phpPassword = '9J$1#Q-(XjUNJe';
$phpDatabase = 'ffa_asean_php_test';

$thaHostName = 'ffasqldbqa-impvprj.mysql.database.azure.com';
$thaUserName = 'ffasqldbqaimp@ffasqldbqa-impvprj';
$thaPassword = '9J$1#Q-(XjUNJe';
$thaDatabase = 'ffa_asean_tha_test';

$vnHostName = 'ffasqldbqa-impvprj.mysql.database.azure.com';
$vnUserName = 'ffasqldbqaimp@ffasqldbqa-impvprj';
$vnPassword = '9J$1#Q-(XjUNJe';
$vnDatabase = 'ffa_asean_vn_test';

$analyticalHostName = 'ffa-mssql-svr-qa.database.windows.net';
$analyticalUserName = 'ffamssqlsvrqaadmin';
$analyticalPassword = '4kuZ96F99,2s';
$analyticalDatabase = 'ffa-mssql-qa-analytical';

switch ($argv) {
    case in_array('--dev', $argv):
        $jktHostName = 'ffa-sqlsvr-dev-impvprj.mysql.database.azure.com';
        $jktUserName = 'ffasqlsvrimpvprj@ffa-sqlsvr-dev-impvprj';
        $jktPassword = 'aRYRJF!YJ)f4';
        $jktDatabase = 'ffa_asean_jkt_test';

        $myHostName = 'ffa-sqlsvr-dev-impvprj.mysql.database.azure.com';
        $myUserName = 'ffasqlsvrimpvprj@ffa-sqlsvr-dev-impvprj';
        $myPassword = 'aRYRJF!YJ)f4';
        $myDatabase = 'ffa_asean_mys_test';

        $phpHostName = 'ffa-sqlsvr-dev-impvprj.mysql.database.azure.com';
        $phpUserName = 'ffasqlsvrimpvprj@ffa-sqlsvr-dev-impvprj';
        $phpPassword = 'aRYRJF!YJ)f4';
        $phpDatabase = 'ffa_asean_php_test';

        $thaHostName = 'ffa-sqlsvr-dev-impvprj.mysql.database.azure.com';
        $thaUserName = 'ffasqlsvrimpvprj@ffa-sqlsvr-dev-impvprj';
        $thaPassword = 'aRYRJF!YJ)f4';
        $thaDatabase = 'ffa_asean_tha_test';

        $vnHostName = 'ffa-sqlsvr-dev-impvprj.mysql.database.azure.com';
        $vnUserName = 'ffasqlsvrimpvprj@ffa-sqlsvr-dev-impvprj';
        $vnPassword = 'aRYRJF!YJ)f4';
        $vnDatabase = 'ffa_asean_vn_test';

        break;

    case in_array('--staging', $argv):
        $jktHostName = 'ffasqldbqa-impvprj.mysql.database.azure.com';
        $jktUserName = 'ffasqldbqaimp@ffasqldbqa-impvprj';
        $jktPassword = '9J$1#Q-(XjUNJe';
        $jktDatabase = 'ffa_asean_jkt_test';

        $myHostName = 'ffasqldbqa-impvprj.mysql.database.azure.com';
        $myUserName = 'ffasqldbqaimp@ffasqldbqa-impvprj';
        $myPassword = '9J$1#Q-(XjUNJe';
        $myDatabase = 'ffa_asean_mys_test';

        $phpHostName = 'ffasqldbqa-impvprj.mysql.database.azure.com';
        $phpUserName = 'ffasqldbqaimp@ffasqldbqa-impvprj';
        $phpPassword = '9J$1#Q-(XjUNJe';
        $phpDatabase = 'ffa_asean_php_test';

        $thaHostName = 'ffasqldbqa-impvprj.mysql.database.azure.com';
        $thaUserName = 'ffasqldbqaimp@ffasqldbqa-impvprj';
        $thaPassword = '9J$1#Q-(XjUNJe';
        $thaDatabase = 'ffa_asean_tha_test';

        $vnHostName = 'ffasqldbqa-impvprj.mysql.database.azure.com';
        $vnUserName = 'ffasqldbqaimp@ffasqldbqa-impvprj';
        $vnPassword = '9J$1#Q-(XjUNJe';
        $vnDatabase = 'ffa_asean_vn_test';
        break;

    case in_array('--analytical', $argv):
    
        $analyticalHostName = 'ffa-mssql-svr-qa.database.windows.net';
        $analyticalUserName = 'ffamssqlsvrqaadmin';
        $analyticalPassword = '4kuZ96F99,2s';
        $analyticalDatabase = 'ffa-mssql-qa-analytical';
        break;

    default:
        $jktHostName = 'ffasqldbqa-impvprj.mysql.database.azure.com';
        $jktUserName = 'ffasqldbqaimp@ffasqldbqa-impvprj';
        $jktPassword = '9J$1#Q-(XjUNJe';
        $jktDatabase = 'ffa_asean_jkt_test';

        $myHostName = 'ffasqldbqa-impvprj.mysql.database.azure.com';
        $myUserName = 'ffasqldbqaimp@ffasqldbqa-impvprj';
        $myPassword = '9J$1#Q-(XjUNJe';
        $myDatabase = 'ffa_asean_mys_test';

        $phpHostName = 'ffasqldbqa-impvprj.mysql.database.azure.com';
        $phpUserName = 'ffasqldbqaimp@ffasqldbqa-impvprj';
        $phpPassword = '9J$1#Q-(XjUNJe';
        $phpDatabase = 'ffa_asean_php_test';

        $thaHostName = 'ffasqldbqa-impvprj.mysql.database.azure.com';
        $thaUserName = 'ffasqldbqaimp@ffasqldbqa-impvprj';
        $thaPassword = '9J$1#Q-(XjUNJe';
        $thaDatabase = 'ffa_asean_tha_test';

        $vnHostName = 'ffasqldbqa-impvprj.mysql.database.azure.com';
        $vnUserName = 'ffasqldbqaimp@ffasqldbqa-impvprj';
        $vnPassword = '9J$1#Q-(XjUNJe';
        $vnDatabase = 'ffa_asean_vn_test';
}

if (
    isset($jktUserName) && isset($jktHostName) && isset($jktPassword) && isset($jktDatabase) &&
    isset($myUserName) && isset($myHostName) && isset($myPassword) && isset($myDatabase) &&
    isset($phpUserName) && isset($phpHostName) && isset($phpPassword) && isset($phpDatabase) &&
    isset($thaUserName) && isset($thaHostName) && isset($thaPassword) && isset($thaDatabase) &&
    isset($vnUserName) && isset($vnHostName) && isset($vnPassword) && isset($vnDatabase) &&
    isset($analyticalHostName) &&
    isset($analyticalUserName) &&
    isset($analyticalPassword) &&
    isset($analyticalDatabase)
) {
    define('FFA_JKT_HOSTNAME', $jktHostName);
    define('FFA_JKT_USERNAME', $jktUserName);
    define('FFA_JKT_PASSWORD', $jktPassword);
    define('FFA_JKT_DATABASE', $jktDatabase);
    
    define('FFA_MY_HOSTNAME', $myHostName);
    define('FFA_MY_USERNAME', $myUserName);
    define('FFA_MY_PASSWORD', $myPassword);
    define('FFA_MY_DATABASE', $myDatabase);
    
    define('FFA_PH_HOSTNAME', $phpHostName);
    define('FFA_PH_USERNAME', $phpUserName);
    define('FFA_PH_PASSWORD', $phpPassword);
    define('FFA_PH_DATABASE', $phpDatabase);
    
    define('FFA_THA_HOSTNAME', $thaHostName);
    define('FFA_THA_USERNAME', $thaUserName);
    define('FFA_THA_PASSWORD', $thaPassword);
    define('FFA_THA_DATABASE', $thaDatabase);
    
    define('FFA_VN_HOSTNAME', $vnHostName);
    define('FFA_VN_USERNAME', $vnUserName);
    define('FFA_VN_PASSWORD', $vnPassword);
    define('FFA_VN_DATABASE', $vnDatabase);

    define('ANALYTICAL_HOSTNAME', $analyticalHostName);
    define('ANALYTICAL_USERNAME', $analyticalUserName);
    define('ANALYTICAL_PASSWORD', $analyticalPassword);
    define('ANALYTICAL_DATABASE', $analyticalDatabase);

} else {
    echo Logs::error("Incomplete Database Credentials");
    exit();
}