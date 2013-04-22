HDInsight
=========

HDInsight : Hadoop - Hive - Microsoft Business Intelligence

The UDF Explode() take an array and explode it : SELECT EXPLODE(COL) AS MyCOL FROM MyTable;

     Col  		   MyCOL
  | [1,2] |	->	|  1  |
  | [3,4] |		  |  2  |
  				      |  3  |
  				      |  4  |

With LATERAL VIEW : SELECT ID, MyCol FROM MyTable LATERAL VIEW EXPLODE(Col) MyVirtualTable AS MyCol;

    ID    Col          ID  MyCol
   | 1 | [1,2] |  ->  | 1 |  1  |
   | 2 | [3,4] |      | 1 |  2  |
                      | 2 |  3  |
                      | 2 |  4  |

With Multiple LATERAL VIEW : 
  SELECT MyCol1, MyCol2 FROM MaTable 
  LATERAL VIEW explode(Col1) MyVirtualTable1 AS MyCol1
  LATERAL VIEW explode(Col2) MyVirtualTable2 AS MyCol2;

      Col1    Col2          MyCol1  MyCol2
   | [1,2] | [5,6] |  ->  |   1   |   5   |
   | [3,4] | [7,8] |      |   1   |   6   |
                          |   2   |   5   |
                          |   2   |   6   |
                          |   3   |   7   |
                          |   3   |   8   |
                          |   4   |   7   |
                          |   4   |   8   |

!!! With MyExplode() UDF :
  SELECT MyCol1, MyCol2 FROM MyTable 
  LATERAL VIEW MyExplode(Array(1,2,3), Array(4,5,6)) MyVirtualTable As MyCol1, MyCol2;

       Col1      Col2           MyCol1  MyCol2
   | [1,2,3] | [4,5,6] |  ->  |   1   |   4   |
                              |   2   |   5   |
                              |   3   |   6   |

-------------------------------------------------------------------------------------------------------------
Instalation : 

hive>  ADD JAR /#PATH#/MyExplode.jar;
hive>  CREATE TEMPORARY FUNCTION MyExplode AS 'com.example.hive.udf.MyExplode';

