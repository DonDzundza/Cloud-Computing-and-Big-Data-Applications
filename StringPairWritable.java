package stubs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class StringPairWritable implements WritableComparable<StringPairWritable> {

  String left;
  String right;

  /**
   * Empty constructor - required for serialization.
   */ 
  public StringPairWritable() {

  }

  /**
   * Constructor with two String objects provided as input.
   */ 
  public StringPairWritable(String left, String right) {
    this.left = left;
    this.right = right;    
  }

  /**
   * Serializes the fields of this object to out.
   */
  public void write(DataOutput out) throws IOException {
    
    /*
     * TODO implement
     */
	  out.writeUTF(this.left);
	  out.writeUTF(this.right);

    
  }

  /**
   * Deserializes the fields of this object from in.
   */
  public void readFields(DataInput in) throws IOException {
    
    /*
     * TODO implement
     */
    this.left = in.readUTF();
    this.right = in.readUTF();
    
  }

  /**
   * Compares this object to another StringPairWritable object by
   * comparing the left strings first. If the left strings are equal,
   * then the right strings are compared.
   */
  public int compareTo(StringPairWritable other) {
    int ret = 0;
    if(this.left.hashCode() != other.left.hashCode()){
//    	return (this.left.hashCode() < other.left.hashCode() ? -1:1);
    	if (this.left.compareTo(other.left) < 0){
    		return -1;
    	} else {
    		return 1;
    	}
    }
    else if (this.right.hashCode() != other.right.hashCode()){
    	if (this.right.compareTo(other.right) < 0){
    		return -1;
    	} else {
    		return 1;
    	}
    }
    return ret;
  }

  /**
   * A custom method that returns the two strings in the 
   * StringPairWritable object inside parentheses and separated by
   * a comma. For example: "(left,right)".
   */
  public String toString() {
     return "(" + left + "," + right + ")";
  }

@Override
public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + ((left == null) ? 0 : left.hashCode());
	result = prime * result + ((right == null) ? 0 : right.hashCode());
	return result;
}

@Override
public boolean equals(Object obj) {
	if (this == obj)
		return true;
	if (obj == null)
		return false;
	if (getClass() != obj.getClass())
		return false;
	StringPairWritable other = (StringPairWritable) obj;
	if (left == null) {
		if (other.left != null)
			return false;
	} else if (!left.equals(other.left))
		return false;
	if (right == null) {
		if (other.right != null)
			return false;
	} else if (!right.equals(other.right))
		return false;
	return true;
}

//  @Override
//public boolean equals(Object obj) {
//	if (obj instanceof StringPairWritable){
//		StringPairWritable other = (StringPairWritable) obj;
//		return this.left == other.left && this.right == other.right;
//	}
//	return false;
//}
//
//  @Override
//public int hashCode() {
//	final int prime = 31;
//	int result = 1;
//	String l = this.left;
//	String r = this.right;
//	int lNum = 0;
//	int rNum = 0;
//	for(int i = 0; i<l.length(); i++){
//		lNum += l.charAt(i);
//	}
//	for(int j = 0; j<r.length(); j++){
//		rNum += r.charAt(j);
//	}
//	result = prime * lNum + rNum;
//	return result;
//}
}
