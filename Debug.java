package jutil;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;

public class Debug {
	private static boolean isEnabled = true;

	public static void expose(Object obj) {
		if(!isEnabled)
			return;

		Debug.pr("Object");
		try{
			Debug.pr(obj.getClass().getName());
		}catch(Exception e){
			Debug.pr(e.getMessage());
		}

		if(obj == null ){
			System.out.println("<NULL>");
			return;
		}

		System.out.println(obj);

	}

	public static void expose(JSONObject jsonObj) {
		if(!isEnabled)
			return;

		System.out.println("JSONObject");

		if(jsonObj == null ){
			System.out.println("<NULL>");
			return;
		}
		if(jsonObj.length() > 0)
			expose(jsonObj, 0);
		else
			pr("<empty JSON Object>");
	}

	public static void expose(JSONObject jsonObj, int depth) {
		if(!isEnabled)
			return;

		System.out.println("JSONObject");

		if(jsonObj == null ){
			System.out.println("<NULL>");
			return;
		}
		String leadingSpaces = "";
		for(int i = 0; i<jsonObj.names().length(); i++){
		    System.out.println(leadingSpaces + jsonObj.names().getString(i) + " : " + jsonObj.get(jsonObj.names().getString(i)));
		}
	}

	public static void expose(JSONArray jsonArr) {
		if(!isEnabled)
			return;

		System.out.println("JSONArray");

		if(jsonArr == null ){
			System.out.println("<NULL>");
			return;
		}
		pr(jsonArr);

		for(int i = 0; i < jsonArr.length(); i++) {
			Object obj = jsonArr.get(i);
			expose(obj);
		}
	}

	public static void expose(Set<String> mySet) {
		if(!isEnabled)
			return;

		System.out.println("Set<String>");

		if(mySet == null ){
			System.out.println("<NULL>");
			return;
		}
		for (String myString : mySet) {
		    System.out.println(myString);
		}
	}


	public static void expose(String[] myStrings) {
		if(!isEnabled)
			return;

		System.out.println("String[]");

		if(myStrings == null ){
			System.out.println("<NULL>");
			return;
		}
		for (String myString : myStrings) {
		    System.out.println(myString);
		}
	}


	public static void expose(Map myMap) {
		if(!isEnabled)
			return;

		System.out.println("Map");

		if(myMap == null ){
			System.out.println("<NULL>");
			return;
		}

		if(myMap.size() == 0)
			Debug.pr("<EMPTY>");
		else{
		    Iterator it = myMap.entrySet().iterator();
		    while (it.hasNext()) {
		        Map.Entry pair = (Map.Entry)it.next();
		        pr(pair.getKey() + " : " + pair.getValue());
		    }
		}
	}

	public static void pr(String myString) {
		if(isEnabled)
			System.out.println(myString);
	}

	public static void pr(Object myObj) {
		if(isEnabled)
			System.out.println(myObj);
	}

	public static void hit(){
		if(!isEnabled)
			return;
		StackTraceElement stack = new Exception().getStackTrace()[1];
		Debug.pr("hit @ "+stack.toString());
	}

	public static void die() {
		if(!isEnabled)
			return;
		Debug.pr("Debug.die()");
		Thread.dumpStack();
		Util.printMemoryUsage();
		System.exit(1);
	}

	public static void die(String msg) {
		Debug.pr(msg);
		Debug.die();
	}

	public static void enable() {
		isEnabled = true;
	}

	public static void disable() {
		isEnabled = false;
	}
}
