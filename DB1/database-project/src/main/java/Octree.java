package main.java;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.*;

public class Octree implements Serializable{
	private static final long serialVersionUID = 3753379390813741157L;
    private static final int MAX_ELEMENTS = readConfig()[1]; // maximum number of elements in a leaf node
    public OctreeNode root;
    public String col1="";
    public String col2="";
    public String col3="";

    public void printAllNodes() {
        printAllNodesRecursive(root);
    }

    private void printAllNodesRecursive(OctreeNode node) {
        if (node == null) {
            return;
        }
        
        // Print the node's boundaries
        System.out.println("Node Boundaries: (" + node.minX + ", " + node.maxX + "), (" +
                node.minY + ", " + node.maxY + "), (" + node.minZ + ", " + node.maxZ + ")");
        
        // Print the elements in the node
        if (node.elements != null) {
            for (OctreeElement element : node.elements) {
                System.out.println("Element: (" + element.x + ", " + element.y + ", " + element.z + ")");
            }
        }
        
        // Recursively print the child nodes
        if (node.children != null) {
            for (OctreeNode child : node.children) {
                printAllNodesRecursive(child);
            }
        }
    }
    
    public static int compareObj(Object o1, Object o2) {
 		 String currDataType= o1.getClass().getName();
 		 int result ;
 		 switch (currDataType) {  
  	 case "java.lang.Integer":{
  		int intValue = Integer.parseInt(o2.toString());

  		 if ((int) o1 == intValue) {

  	        result = 0;
  	    }
  		 else if ((int)o1<intValue) {
  				 result= -1;
  		 }
  		 else {
  			 result =  1; }
  		 break;
  	 }
  	 case "java.lang.String":{
  			 result = ((String) o1).compareTo((String)o2);    		
  		 break;
  	 }
  	 case "java.lang.Double":{
  		 if(Double.compare((Double)o1, (Double)o2) == 0) {
   			result = 0;
   		 }
   		 else if ((Double)o1<(Double)o2) {
   				 result= -1;
   		 }
   		 else {
   			 result =  1; }
   		 break;
  	 }
  	 case "java.util.Date":{
             result =((Date) o1).compareTo((Date)o2);
  		break;
  	 }
  	 default : result =0;
  	 break;
  	 }
 		return result;
 	}
    
    private static int[] readConfig() {
        Properties prop = new Properties();
        String filePath = "src/main/resources/DBApp.config";
        
        InputStream is = null;
        try {
            is = new FileInputStream(filePath);
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        }
        try {
            prop.load(is);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        int[] arr = new int[2];
        arr[0] = Integer.parseInt(prop.getProperty("MaximumRowsCountinTablePage"));
        arr[1] = Integer.parseInt(prop.getProperty("MaximumEntriesinOctreeNode"));
        return arr;
    }	

	public Octree(Object minX, Object maxX, Object minY, Object maxY, Object minZ, Object maxZ) {
        this.setRoot(new OctreeNode(minX, maxX, minY, maxY, minZ, maxZ));
    }

    public void insert(Object x, Object y, Object z, String reference) {
        getRoot().insert(x, y, z, reference);
    }

    public void remove(Object x, Object y, Object z) {
        removeElement(getRoot(), x, y, z);
    }

    private void removeElement(OctreeNode node, Object x, Object y, Object z) {
        if (node.children == null) {
            List<OctreeElement> iterator = node.elements;
            for(int i=0;i<iterator.size();i++) {
                OctreeElement element = iterator.get(i);
                if (compareObj(element.x, x) == 0 &&
                    compareObj(element.y, y) == 0 &&
                    compareObj(element.z, z) == 0) {
                    iterator.remove(i);
                    node.elements= iterator;
                }
            }
        } else {
        	int index = node.getChildIndex(x, y, z);
            removeElement(node.children[index], x, y, z);

            // Check if the child node became empty after the removal
            if (node.children[index].isEmpty()) {
                node.children[index] = null;
        }}

    }

    public List<String> searchElements(OctreeNode node, Object x, Object y, Object z, String opx, String opy, String opz) {
        List<String> references = new ArrayList<>();
        searchElementsRecursive(node, x, y, z,opx,opy,opz, references);
        return references;
    }

    public void searchElementsRecursive(OctreeNode node, Object x, Object y, Object z, String opx, String opy, String opz, List<String> references) {
        if (node.children == null) {
            for (OctreeElement element : node.elements) {
                if (compareObjWithOperator(element.x, x, opx) &&
                    compareObjWithOperator(element.y, y, opy) &&
                    compareObjWithOperator(element.z, z, opz)) {
                    references.add(element.reference);
                }
            }
        } else {
            int index = node.getChildIndex(x, y, z);
            if (node.children[index] != null) {
                searchElementsRecursive(node.children[index], x, y, z, opx, opy, opz, references);
            }
        }
    }

    private boolean compareObjWithOperator(Object obj1, Object obj2, String operator) {
        switch (operator) {
            case "=":
                return obj1.equals(obj2);
            case "<":
                return compareObj(obj1, obj2) < 0;
            case ">":
                return compareObj(obj1, obj2) > 0;
            case "<=":
                return compareObj(obj1, obj2) <= 0;
            case ">=":
                return compareObj(obj1, obj2) >= 0;
            case "!=":
                return !obj1.equals(obj2);
            default:
                return false; // or handle as desired, e.g., throw an exception
        }
    }

    
    
    public OctreeNode getRoot() {
		return root;
	}

	public void setRoot(OctreeNode root) {
		this.root = root;
	}

	public static class OctreeElement  implements Serializable{
        public final Object x, y, z;
        public final String reference;

        public OctreeElement(Object x, Object y, Object z, String reference) {
            this.x = x;
            this.y = y;
            this.z = z;
            this.reference = reference;
        }
    }
    
    public static class OctreeNode implements Serializable {

    	public final Object minX, maxX, minY, maxY, minZ, maxZ;
    	public List<OctreeElement> elements;
    	public OctreeNode[] children;

        public OctreeNode(Object minX, Object maxX, Object minY, Object maxY, Object minZ, Object maxZ) {
            this.minX = minX;
            this.maxX = maxX;
            this.minY = minY;
            this.maxY = maxY;
            this.minZ = minZ;
            this.maxZ = maxZ;
            this.elements = new ArrayList<OctreeElement>();
            this.children = null;
        }
        
        public boolean isEmpty() {
            if (children == null) {
                return elements.isEmpty();
            } else {
                for (OctreeNode child : children) {
                    if (child != null && !child.isEmpty()) {
                        return false;
                    }
                }
                return true;
            }
        } 

        public void insert(Object x, Object y, Object z, String ref) {
            if (children == null) {
                if (elements.size() < MAX_ELEMENTS) {
                    elements.add(new OctreeElement(x, y, z, ref)); //reference of tuple
                } else {
                    split();
                    insert(x, y, z, ref);
                }
            } else {
                int index = getChildIndex(x, y, z);
                children[index].insert(x, y, z, ref);
            }
        }


        private void split() {
            children = new OctreeNode[8];
            Object midX = calculateMidpoint(minX , maxX);
            Object midY = calculateMidpoint(minY , maxY);
            Object midZ = calculateMidpoint(minZ , maxZ);
            children[0] = new OctreeNode(minX, midX, minY, midY, minZ, midZ);
            children[1] = new OctreeNode(midX, maxX, minY, midY, minZ, midZ);
            children[2] = new OctreeNode(minX, midX, midY, maxY, minZ, midZ);
            children[3] = new OctreeNode(midX, maxX, midY, maxY, minZ, midZ);
            children[4] = new OctreeNode(minX, midX, minY, midY, midZ, maxZ);
            children[5] = new OctreeNode(midX, maxX, minY, midY, midZ, maxZ);
            children[6] = new OctreeNode(minX, midX, midY, maxY, midZ, maxZ);
            children[7] = new OctreeNode(midX, maxX, midY, maxY, midZ, maxZ);

            for (OctreeElement element : elements) {
                int index = getChildIndex(element.x, element.y, element.z);
                children[index].insert(element.x, element.y, element.z, element.reference);
            }
            elements = new ArrayList<OctreeElement>();
        }

        public int getChildIndex(Object x, Object y, Object z) {
            int index = 0;
            if (compareObj(x, calculateMidpoint(minX , maxX)) >= 0) index |= 1;
            if (compareObj(y, calculateMidpoint(minY , maxY) ) >= 0) index |= 2;
            if (compareObj(z, calculateMidpoint(minZ , maxZ) ) >= 0) index |= 4;
            return index;
        }

        
        public boolean containsPoint(Object x, Object y, Object z) {
            return compareObj(x, minX) >= 0 && compareObj(x, maxX) <= 0 &&
                   compareObj(y, minY) >= 0 && compareObj(y, maxY) <= 0 &&
                   compareObj(z, minZ) >= 0 && compareObj(z, maxZ) <= 0;
        }
        
        public static int compareObj(Object o1, Object o2) {
		 String currDataType= o1.getClass().getName();
		 int result ;
		 switch (currDataType) { 
 	 case "java.lang.Integer":{
 		 if((int)o1==(int)o2) {
 			result = 0;
 		 }
 		 else if ((int)o1<(int)o2) {
 				 result= -1;
 		 }
 		 else {
 			 result =  1; }
 		 break;
 	 }
 	 case "java.lang.String":{
 			 result = ((String) o1).compareTo((String)o2);    		
 		 break;
 	 }
 	 case "java.lang.Double":{
 		 if((Double)o1==(Double)o2) {
  			result = 0;
  		 }
  		 else if ((Double)o1<(Double)o2) {
  				 result= -1;
  		 }
  		 else {
  			 result =  1; }
  		 break;
 	 }
 	 case "java.util.Date":{
            result =((Date) o1).compareTo((Date)o2);
 		break;
 	 }
 	 default : result =0;
 	 break;
 	 }
		return result;
	}
       
        public static Object calculateMidpoint(Object minX, Object maxX) {
        if (minX instanceof Integer && maxX instanceof Integer) {
            return (int) ((int) minX + (int) maxX) / 2;
        } else if (minX instanceof Double && maxX instanceof Double) {
            return (double) ((double) minX + (double) maxX) / 2;
        } else if (minX instanceof String && maxX instanceof String) {
            String minString = (String) minX;
            String maxString = (String) maxX;
            int minLength = minString.length();
            int maxLength = maxString.length();
            int commonLength = Math.min(minLength, maxLength);
            int diffIndex = -1;
            
            for (int i = 0; i < commonLength; i++) {
                if (minString.charAt(i) != maxString.charAt(i)) {
                    diffIndex = i;
                    break;
                }
            }
            
            if (diffIndex == -1) {
                if (minLength < maxLength) {
                    return minString + maxString.substring(minLength);
                } else {
                    return maxString + minString.substring(maxLength);
                }
            } else {
                char minChar = minString.charAt(diffIndex);
                char maxChar = maxString.charAt(diffIndex);
                int midChar = (minChar + maxChar) / 2;
                return minString.substring(0, diffIndex) + (char) midChar;
            }
        } else if (minX instanceof Date && maxX instanceof Date) {
            long midpoint = ((Date) minX).getTime() + (((Date) maxX).getTime() - ((Date) minX).getTime()) / 2;
            return new Date(midpoint);
        } else {
            // throw an exception for unsupported data types
            throw new IllegalArgumentException("Unsupported data types: " + minX.getClass().getName() + ", " + maxX.getClass().getName());
        }
    }


    }}


