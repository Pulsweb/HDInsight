package com.example.hive.udf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

@Description(name = "MyExplode",
	value = "_FUNC_(a, b) - separates the elements of arrays a and b into multiple rows")

public class MyExplode extends GenericUDTF {

  private ListObjectInspector listOI0 = null;
  private ListObjectInspector listOI1 = null;

  @Override
  	public void close() throws HiveException {
  }

  @Override
  public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
   
    if (args[0].getCategory() != ObjectInspector.Category.LIST) {
      throw new UDFArgumentException("explode() takes an array as a parameter");
    }
    listOI0 = (ListObjectInspector) args[0];
    listOI1 = (ListObjectInspector) args[1];

    ArrayList<String> fieldNames = new ArrayList<String>();
    ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
    fieldNames.add("col0");
    fieldNames.add("col1");
    fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING));
    fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING));
    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
  }

  private final Object[] forwardObj = new Object[2];

  @Override
  public void process(Object[] o) throws HiveException {
    List<?> list0 = listOI0.getList(o[0]);
    List<?> list1 = listOI1.getList(o[1]);
    if(list0 == null) {
      return;
    }
    for(int i=0; i< list0.size(); i++)
    {
     Object r0 = list0.get(i);
     Object r1 = list1.get(i);
     forwardObj[0] = r0.toString();
     forwardObj[1] = r1.toString();
       forward(forwardObj);
    }
  }

  @Override
  public String toString() {
    return "MyExplode";
  }
}