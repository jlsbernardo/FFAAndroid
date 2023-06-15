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

    protected $limitPerPage = 20000;

    protected $limitChunked = 1000;

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
        //Get total count for chunking 
        $totalCountQuery = $this->getDataFromFFAQuery(1);
        $resultTotalCount = $this->exec_query($totalCountQuery);
        if ($resultTotalCount->num_rows > 0) {
            $resultTotalCountRow = $resultTotalCount->fetch_assoc();
            $totalCount = $resultTotalCountRow['total_count'];
        }
        if($totalCount == 0) {
            $message = "No Products Records to sync";
            return [
                'data'  => $message,
            ];
        }
            
        $query = $this->getDataFromFFAQuery();
        $queryList = populate_chunked_query_list($query, $totalCount, $this->limitPerPage);
        $userInsertQuery = "";
        $data = [];
        $country = $this->country['country_name'];
        foreach ($queryList as $query) {
            $result = $this->exec_query($query); 
            if ($result->num_rows > 0) {
                $rows = $result->fetch_all(MYSQLI_ASSOC);
                foreach($rows as $row) {
                    $objVN = new vn_charset_conversion();
                    $product_name = (strtolower($country)=='vietnam') ? $objVN->convert(    $row['product_name']    ) : $row['product_name'];
                    $product_cat = (strtolower($country)=='vietnam') ? $objVN->convert(    $row['product_category']    ) : $row['product_category'];
                    $productRNAFields = array(
                        'ffa_id'            => $row['product_code'],
                        'deleted'           => 0,
                        'report_table'      => $this->reportTable,
                        'product_name'      => $product_name,
                        'product_category'  => $product_cat,
                    );

                    $data[] = $productRNAFields;
                }
            }
        } 

        return [
            'data'  => $data
        ];
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
        $dataChunkeds = array_chunk($data, $this->limitChunked);

        foreach ($dataChunkeds as $dataChunked) {
            $ffaIds = array_column($dataChunked, 'ffa_id');
            $ffaRecordStagingList = $this->getRecordStagingList($ffaIds);
            $strColumns = implode(', ', array_keys($dataChunked[0]));
            $insertQuery = "INSERT INTO [$this->schemaName].[$this->stagingTable] ({$strColumns}) VALUES";
            $insertQueryValue = [];

            foreach ($dataChunked as $productRNAFields) {
                $isInsertedStaging = $this->isInsertedStaging($ffaRecordStagingList, $productRNAFields['ffa_id']);
                if (!$isInsertedStaging) {
                    $strValues =  " '" . implode("', '", array_values($productRNAFields)) . "' ";
                    $insertQueryValue[] = "({$strValues})";
                    $count++;
                } else {
                    $ffaId = $productRNAFields['ffa_id'];

                    $updateQuery = "
                            UPDATE [$this->schemaName].[$this->stagingTable]
                            SET 
                                [product_name] = '{$productRNAFields['product_name']}',
                                [product_category] = '{$productRNAFields['product_category']}',
                                deleted      = 0
                        WHERE [ffa_id] = '$ffaId' AND [report_table] = '$this->reportTable';";

                    $result =  $this->exec_query($updateQuery);
                    if ($result) {
                        $count += 1;
                    }
                }
            }

            if(!empty($insertQueryValue)) {
                $insertQuery .= implode(',', $insertQueryValue);
                $result = $this->exec_query($insertQuery);
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

    public function getDataFromFFAQuery($isCount = 0) {
        $select = "SELECT
            product_code,
            product_name,
            product_category";
        if($isCount) {
            $select = "SELECT COUNT(*) OVER () AS total_count";
        }

        $sql = "{$select}
                FROM
                $this->ffaTable";
        if($isCount) {
            $sql .= " LIMIT 1";
        }
        return $sql;
    }
    
}
