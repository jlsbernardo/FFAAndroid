<?php

require_once(dirname(__FILE__, 3) . '/src/RNA/Database/DB.php');

class Product extends DB
{

    protected $ffaTable = 'tbl_mrp_list';

    protected $stagingTable = '';

    protected $reportTable = 'products';

    protected $country = '';

    protected $connection;

    protected $ffaSyncTable = 'tbl_rna_etl_sync';

    protected $schemaName = 'analytical';

    public function __construct($country = '', $connection = 'ffa')
    {
        $this->country = $country;
        if ($country != '') {
            $this->stagingTable = 'staging_' . $country['short_name'];
        }

        $this->connection = $connection;
        parent::__construct();
    }

    public function getDataFromFFA()
    {
        $sql = "SELECT
            product_code,
            product_name,
            product_category
        FROM
            $this->ffaTable";  

        $userInsertQuery = "";
        $result = $this->exec_query($sql);
        $data = [];
        $country = $this->country['country_name'];
        if ($result->num_rows > 0) {
            while ($row = $result->fetch_assoc()) {
                $objVN = new vn_charset_conversion();
                $product_name = (strtolower($country)=='vietnam') ? $objVN->convert(    $row['product_name']    ) : $row['product_name'];
                $product_cat = (strtolower($country)=='vietnam') ? $objVN->convert(    $row['product_category']    ) : $row['product_category'];
                $cropsRnaFields = array(
                    'ffa_id'            => $row['product_code'],
                    'deleted'           => 0,
                    'report_table'      => $this->reportTable,
                    'product_name'      => $product_name,
                    'product_category'  => $product_cat,
                );

                $data[] = $cropsRnaFields;
            }

            return [
                'data'  => $data
            ];
        } else {
            $message = "No Products Records to sync";
            return [
                'data'  => $message
            ];
        }
    }

     /**
     * Insert to staging table
     *
     * @param $data
     * @return array
     */
    public function insertIntoStaging($data)
    {
        $count = 0;

        foreach ($data as $cropsRNAFields) {
            $results = $this->__checkRecordStaging($cropsRNAFields);
            if (sqlsrv_num_rows($results) < 1) {
                $strColumns = implode(', ', array_keys($cropsRNAFields));
                $strValues =  " '" . implode("', '", array_values($cropsRNAFields)) . "' ";
                $complaintsInsertQuery = "INSERT INTO [$this->schemaName].[$this->stagingTable] ({$strColumns}) VALUES ({$strValues});";

                $result = $this->exec_query($complaintsInsertQuery);
                if ($result) {
                    $count += 1;
                }
            } else {
                
                $ffaId = $cropsRNAFields['ffa_id'];

                $cropsUpdateQuery = "
                        UPDATE [$this->schemaName].[$this->stagingTable]
                        SET 
                            [product_name] = '{$cropsRNAFields['product_name']}',
                            [product_category] = '{$cropsRNAFields['product_category']}',
                            deleted      = 0
                       WHERE [ffa_id] = '$ffaId' AND [report_table] = '$this->reportTable';";

                $result =  $this->exec_query($cropsUpdateQuery);
                if ($result) {
                    $count += 1;
                }
            }
        }

        return [
            'count' => $count
        ];
    }

    /**
     * Checking record in staging table
     *
     * @param $data
     * @return object | array
     */
    private function __checkRecordStaging($data)
    {
        $ffaId = $data['ffa_id'];
        $sql = "SELECT TOP 1 ffa_id, report_table 
            FROM  
            [$this->schemaName].[$this->stagingTable]
            WHERE report_table = '$this->reportTable' AND ffa_id = '$ffaId' ORDER BY id desc";
        $res = $this->exec_query($sql);

        return $res;
    }
    
}
