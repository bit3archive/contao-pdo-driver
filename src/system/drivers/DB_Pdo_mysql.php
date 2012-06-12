<?php if (!defined('TL_ROOT')) die('You cannot access this file directly!');

/**
 * PDO Database driver for Contao Open Source CMS.
 *
 * @package    Driver
 * @license    LGPL
 * @filesource
 */


require(TL_ROOT . '/system/drivers/DB_Mysql.php');

/**
 * Class DB_PDO_Mysql
 *
 * PDO Driver class for MySQL databases.
 *
 * @author     Tristan Lins <tristan.lins@infinitysoft.de>
 * @package    Driver
 */
class DB_PDO_Mysql extends DB_Mysql
{
	/**
	 * @var PDO
	 */
	protected $resConnection;

	public function prepare($strQuery)
	{
		$objStmt = new DB_PDO_Mysql_Statement($this->resConnection, $this->blnDisableAutocommit);
		return $objStmt->prepare($strQuery);
	}

	public function execute($strQuery)
	{
		return $this->prepare($strQuery)->execute();
	}

	public function executeUncached($strQuery)
	{
		return $this->prepare($strQuery)->execute();
	}

	public function query($strQuery)
	{
		return $this->prepare($strQuery)->execute();
	}

	/**
	 * Abstract database driver methods
	 */
	protected function connect()
	{
		$strDSN = 'mysql:';
		$strDSN .= 'dbname=' . $GLOBALS['TL_CONFIG']['dbDatabase'] . ';';
		$strDSN .= 'host=' . $GLOBALS['TL_CONFIG']['dbHost'] . ';';
		$strDSN .= 'port=' . $GLOBALS['TL_CONFIG']['dbPort'] . ';';
		$strDSN .= 'charset=' . $GLOBALS['TL_CONFIG']['dbCharset'] . ';'; // supported only in PHP 5.3.6+

		$arrOptions = array(
			PDO::ATTR_PERSISTENT => $GLOBALS['TL_CONFIG']['dbPconnect'],
			PDO::MYSQL_ATTR_INIT_COMMAND => 'SET sql_mode=\'\'; SET NAMES ' . $GLOBALS['TL_CONFIG']['dbCharset'],
			PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
		);

		$this->resConnection = new PDO($strDSN, $GLOBALS['TL_CONFIG']['dbUser'], $GLOBALS['TL_CONFIG']['dbPass'], $arrOptions);
	}

	protected function disconnect()
	{
		unset ($this->resConnection);
	}

	protected function get_error()
	{
		$errorInfo = $this->resConnection->errorInfo();
		return $errorInfo[2];
	}

	protected function begin_transaction()
	{
		$this->resConnection->beginTransaction();
	}

	protected function commit_transaction()
	{
		$this->resConnection->commit();
	}

	protected function rollback_transaction()
	{
		$this->resConnection->rollBack();
	}

	protected function set_database($strDatabase)
	{
		$this->resConnection->query('USE ' . $strDatabase);
	}

	protected function lock_tables($arrTables)
	{
		$arrLocks = array();

		foreach ($arrTables as $table=>$mode)
		{
			$arrLocks[] = $table .' '. $mode;
		}

		$this->resConnection->query("LOCK TABLES " . implode(', ', $arrLocks));
	}

	protected function unlock_tables()
	{
		$this->resConnection->query("UNLOCK TABLES");
	}

	protected function get_size_of($strTable)
	{
		$objStatus = $this->resConnection
			->query("SHOW TABLE STATUS LIKE '" . $strTable . "'")
			->fetchObject();

		return ($objStatus->Data_length + $objStatus->Index_length);
	}

	protected function get_next_id($strTable)
	{
		$objStatus = $this->resConnection
			->query("SHOW TABLE STATUS LIKE '" . $strTable . "'")
			->fetchObject();

		return $objStatus->Auto_increment;
	}

	protected function createStatement($resConnection, $blnDisableAutocommit)
	{
		return new DB_PDO_Mysql_Statement($resConnection, $blnDisableAutocommit);
	}
}


/**
 * Class DB_PDO_Mysql_Statement
 *
 * PDO Driver class for MySQL databases.
 *
 * @author     Tristan Lins <tristan.lins@infinitysoft.de>
 * @package    Driver
 */
class DB_PDO_Mysql_Statement extends DB_Mysql_Statement
{
	/**
	 * @var PDO
	 */
	protected $resConnection;

	/**
	 * @var PDOStatement
	 */
	protected $resResult;

	/**
	 * @var array
	 */
	protected $arrParams = array();

	/**
	 * Prepare a statement
	 * @param string
	 * @return Database_Statement
	 * @throws Exception
	 */
	public function prepare($strQuery)
	{
		if (!strlen($strQuery))
		{
			throw new Exception('Empty query string');
		}

		$this->resResult = NULL;
		$this->strQuery = $strQuery;

		// Auto-generate the SET/VALUES subpart
		if (strncasecmp($this->strQuery, 'INSERT', 6) === 0 || strncasecmp($this->strQuery, 'UPDATE', 6) === 0)
		{
			$this->strQuery = str_replace('%s', '%p', $this->strQuery);
		}

		return $this;
	}

	/**
	 * Take an associative array and auto-generate the SET/VALUES subpart of a query
	 *
	 * Usage example:
	 * $objStatement->prepare("UPDATE table %s")->set(array('id'=>'my_id'));
	 * will be transformed into "UPDATE table SET id='my_id'".
	 * @param array
	 * @return Database_Statement
	 */
	public function set($arrParams)
	{
		$arrParams = $this->escapeParams($arrParams);

		// INSERT
		if (strncasecmp($this->strQuery, 'INSERT', 6) === 0)
		{
			$strQuery = sprintf('(%s) VALUES (%s)',
								implode(', ', array_keys($arrParams)),
								implode(', ', array_values($arrParams)));
		}

		// UPDATE
		elseif (strncasecmp($this->strQuery, 'UPDATE', 6) === 0)
		{
			$arrSet = array();

			foreach ($arrParams as $k=>$v)
			{
				$arrSet[] = $k . '=' . $v;
			}

			$strQuery = 'SET ' . implode(', ', $arrSet);
		}

		$this->strQuery = str_replace('%p', $strQuery, $this->strQuery);
		return $this;
	}

	/**
	 * Escape the parameters and execute the current statement
	 * @return Database_Result
	 * @throws Exception
	 */
	public function execute()
	{
		$this->arrParams = func_get_args();

		if (is_array($this->arrParams[0]))
		{
			$this->arrParams = array_values($this->arrParams[0]);
		}

		return $this->query();
	}

	/**
	 * Execute the current statement but do not cache the result
	 * @return Database_Result
	 * @throws Exception
	 */
	public function executeUncached()
	{
		$this->arrParams = func_get_args();

		if (is_array($this->arrParams[0]))
		{
			$this->arrParams = array_values($this->arrParams[0]);
		}

		return $this->query();
	}

	/**
	 * Execute a query and return the result object
	 * @param string
	 * @return Database_Result
	 * @throws Exception
	 */
	public function query($strQuery='')
	{
		if (!empty($strQuery))
		{
			$this->strQuery = $strQuery;
		}

		// Make sure there is a query string
		if ($this->strQuery == '')
		{
			throw new Exception('Empty query string');
		}

		// Execute the query
		if (($this->resResult = $this->execute_query()) == false)
		{
			throw new Exception(sprintf('Query error: %s (%s)', $this->error, $this->strQuery));
		}

		// No result set available
		if ($this->resResult->columnCount() == 0)
		{
			$this->debugQuery();
			return $this;
		}

		// Instantiate a result object
		$objResult = $this->createResult($this->resResult, $this->strQuery);
		$this->debugQuery($objResult);

		return $objResult;
	}

	/**
	 * Debug a query
	 * @param Database_Result
	 */
	protected function debugQuery($objResult=null)
	{
		if (!$GLOBALS['TL_CONFIG']['debugMode'])
		{
			return;
		}

		$arrData[] = $this->strQuery;
		$arrData[] = $this->arrParams;

		if ($objResult === null || strncmp(strtoupper($this->strQuery), 'SELECT', 6) !== 0)
		{
			$arrData[] = sprintf('%d rows affected', $this->affectedRows);
			$GLOBALS['TL_DEBUG'][] = $arrData;
		}
		else
		{
			$arrData[] = sprintf('%s rows returned', $objResult->numRows);

			if (($arrExplain = $this->explain()) != false)
			{
				$arrData[] = $arrExplain;
			}

			$GLOBALS['TL_DEBUG'][] = $arrData;
		}
	}

	/**
	 * Abstract database driver methods
	 */
	protected function prepare_query($strQuery)
	{
		return $strQuery;
	}

	protected function string_escape($strString)
	{
		return $this->resConnection->quote($strString);
	}

	protected function execute_query()
	{
		if (is_array($this->arrParams) && count($this->arrParams)) {
			$objStmt = $this->resConnection->prepare($this->strQuery);
			$objStmt->execute($this->arrParams);
		}
		else {
			$objStmt = $this->resConnection->query($this->strQuery);
		}

		return $objStmt;
	}

	protected function get_error()
	{
		$errorInfo = $this->resResult->errorInfo();
		return $errorInfo[2];
	}

	protected function affected_rows()
	{
		return $this->resResult->rowCount();
	}

	protected function insert_id()
	{
		return $this->resConnection->lastInsertId();
	}

	protected function explain_query()
	{
		$objStmt = $this->resConnection
			->prepare('EXPLAIN ' . $this->strQuery);
		$objStmt->execute($this->arrParams);
		return $objStmt->fetch(PDO::FETCH_ASSOC);
	}

	protected function createResult($resResult, $strQuery)
	{
		return new DB_PDO_Mysql_Result($resResult, $strQuery);
	}
}

/**
 * Class DB_PDO_Mysql_Result
 *
 * Driver class for MySQL databases.
 *
 * @author     Tristan Lins <tristan.lins@infinitysoft.de>
 * @package    Driver
 */
class DB_PDO_Mysql_Result extends Database_Result
{
	/**
	 * @var PDOStatement
	 */
	protected $resResult;

	/**
	 * @var array
	 */
	protected $arrParams;

	/**
	 * Validate the connection resource and store the query
	 * @param resource
	 * @param string
	 * @throws Exception
	 */
	public function __construct($resResult, $strQuery, $arrParams = array())
	{
		parent::__construct($resResult, $strQuery);

		$this->arrParams = $arrParams;
	}

	/**
	 * Abstract database driver methods
	 */
	protected function fetch_row()
	{
		return $this->resResult->fetch(PDO::FETCH_NUM);
	}

	protected function fetch_assoc()
	{
		try {
		return $this->resResult->fetch(PDO::FETCH_ASSOC);
		}catch(PDOException $e) {
			echo '<pre>';
			var_dump($this->resResult, $this->strQuery, $this->arrParams);
			echo '</pre>';
			ob_end_flush();
			throw $e;
		}
	}

	protected function num_rows()
	{
		return $this->resResult->rowCount();
	}

	protected function num_fields()
	{
		return $this->resResult->columnCount();
	}

	protected function fetch_field($intOffset)
	{
		return $this->resResult->fetchColumn($intOffset);
	}

	/**
	 * Free the current result
	 */
	public function free()
	{
		unset($this->resResult);
	}
}
