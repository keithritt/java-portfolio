package jutil;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.swing.JFileChooser;
import org.apache.commons.lang3.* ;

public class UI {

	static InputStreamReader conv;
	static BufferedReader buf;
	static boolean alreadyInit = false;
	static String lastResponse;

  public static void main(String[] args){
    String[] options = new String[]{"one", "two", "three"};
	  String test = UI.askWithSearchableOptions("pick one", options);
	}

	public static void init(){
		if(UI.alreadyInit)
			return;

		UI.conv = new InputStreamReader(System.in);
		UI.buf = new BufferedReader(conv);
		UI.alreadyInit = true;
	}

	public static void printBar() {
		UI.print("***********************************");
	}

	public static void print(String msg) {
		System.out.println(msg);
	}

	public static void printClear(String msg) {
		UI.printBar();
		UI.print(msg);
		UI.printBar();
	}

	public static String getUserResponse(){
		UI.init();
		String response;
		try {
			response = UI.buf.readLine();
		} catch (IOException e) {
			Debug.pr(e.getMessage());
			response = "";

			e.printStackTrace();
		}

		UI.lastResponse = response;

		return response;
	}

	public static String ask(String question){
		UI.init();
		UI.print(question);
		return getUserResponse();
	}

	public static Boolean askBoolean(String question){
    	String answer = UI.ask(question + " (y/n)").toLowerCase();
    	switch(answer){
    	case "y":
    	case "yes":
    	case "t":
    	case "true":
    		return true;
    	case "n":
    	case "no":
    	case "f":
    	case "false":
	    	return false;
    	case "exit":
    		UI.exit();
    		return false;
	    default:
	    	UI.print("Invalid response: "+ answer);
	    	UI.print("Please answer 'yes', 'no' or 'exit'");
	    	return UI.askBoolean(question);
    	}
	}

	public static Integer askInteger(String question){
    	String answer = UI.ask(question);
    	try{
    		return Integer.parseInt(answer);
    	}
    	catch(Exception e){
    		UI.print("Invalid response: "+ answer);
    		UI.print("Please enter a valid integer");
    		return UI.askInteger(question);
    	}
	}

	public static String promptForDirectory(String msg){
		String options[] = {
			"Launch File Selector",
			"Type directory manually",
		};

		String answer =  UI.askWithOptions(msg, options);

		String directory = "";
		switch(answer){
		case "Launch File Selector":
			UI.print("Please select the directory from the dialog.");
			directory = UI.launchDirectoryDialog();
			break;
		case "Type directory manually":
			directory = UI.ask("Please enter your directory location.");
			break;
		}

		if(isValidDir(directory))
			return directory;
		else{
			UI.print("Invalid Directory Name: "+ directory);
			return UI.promptForDirectory(msg);
		}
	}

	public static String launchDirectoryDialog(){
		final JFileChooser fc = new JFileChooser();
		fc.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
		fc.setAcceptAllFileFilterUsed(false);
		fc.showOpenDialog(null); //@todo - this constantly pops under eclipse due to null being passed in
		File file = fc.getSelectedFile();
		if(file == null)
			return "";
		else
			return file.toString();
	}

	public static boolean isValidDir(String dirName){
        File dirObj = new File(dirName);
        return dirObj.exists();
	}

	public static String askWithOptions(String question, List<String> fullOptions){
		return UI.askWithOptions(question, Util.getArrayFromList(fullOptions));
	}

	public static String askWithOptions(String question, ArrayList<String> fullOptions){
		return UI.askWithOptions(question, fullOptions.toArray(new String[0]));
	}

	public static String askWithOptions(String question, String[] fullOptions){
		UI.print(question);
		int index = 1;
		Map<Integer, String> shortOptions = new HashMap();
		for(String fullOption: fullOptions){
			// check for an : - this typically separates a command from a description
			String[] parts = fullOption.split(":");
			shortOptions.put(index, parts[0].trim());
			UI.print("     "+index + ": " + fullOption);
			index++;
		}

		String response = UI.getUserResponse();

		if(StringUtils.isNumeric(response)){
			Integer responseIdx = Integer.parseInt(response);

			if(responseIdx <= shortOptions.size() )
				response = shortOptions.get(responseIdx);
		}

		if(!shortOptions.containsValue(response)){
			UI.print("Invalid repsonse: "+response);
			return UI.askWithOptions(question, fullOptions);
		}

		return response;
	}

	public static String askWithSearchableOptions(String question, List<String> fullOptions){
		return UI.askWithSearchableOptions(question, Util.getArrayFromList(fullOptions));
	}

	public static String askWithSearchableOptions(String question, ArrayList<String> fullOptions){
		return UI.askWithSearchableOptions(question, fullOptions.toArray(new String[0]));
	}

	// @todo - this doesnt really have a good way of restoring the full list of options once a search has been performed
	public static String askWithSearchableOptions(String question, String[] fullOptions){
		UI.print(question);
		int index = 1;
		Map<Integer, String> shortOptions = new HashMap();
		for(String fullOption: fullOptions){
			// check for an : - this typically separates a command from a description
			String[] parts = fullOption.split(":");
			shortOptions.put(index, parts[0].trim());
			UI.print("     "+index + ": " + fullOption);
			index++;
		}

		UI.print("     ...or just enter some search criteria");

		String response = UI.getUserResponse();

		if(StringUtils.isNumeric(response)){
			Integer responseIdx = Integer.parseInt(response);

			if(responseIdx <= shortOptions.size() )
				response = shortOptions.get(responseIdx);
		}

		if(!shortOptions.containsValue(response)){
			// go through each original option to search for the text that the user entered
			List<String> filteredOptions = new ArrayList<String>();

			for(String option: fullOptions){
				UI.print(option);
				if(option.toLowerCase().indexOf(response.toLowerCase()) > -1)
					filteredOptions.add(option);
			}

			if(filteredOptions.size() > 0)
				return UI.askWithSearchableOptions(question, filteredOptions);
			else{
				UI.print("That search did not produce any results");
				return UI.askWithSearchableOptions(question, fullOptions);
			}
		}

		return response;
	}

	public static String[] askMultiWithOptions(String question, List<String> fullOptions){
		return UI.askMultiWithOptions(question, Util.getArrayFromList(fullOptions));
	}

	public static String[] askMultiWithOptions(String question, ArrayList<String> fullOptions){
		return UI.askMultiWithOptions(question, fullOptions.toArray(new String[0]));
	}

	public static String[] askMultiWithOptions(String question, String[] fullOptions){
		UI.print(question);
		UI.print("You may select multiple options at once by comma separating your answer.");
		int index = 1;
		Map<Integer, String> shortOptions = new HashMap();
		for(String fullOption: fullOptions){
			// check for an : - this typically separates a command from a description
			String[] parts = fullOption.split(":");
			shortOptions.put(index, parts[0].trim());
			UI.print("     "+index + ": " + fullOption);
			index++;
		}

		String[] responses = UI.getUserResponse().split(",");

		Integer responseCount = 0;
		for(String response : responses){

			response = response.trim();
			if(StringUtils.isNumeric(response)){
				Integer responseIdx = Integer.parseInt(response);

				if(responseIdx <= shortOptions.size() )
					responses[responseCount] = shortOptions.get(responseIdx);
			}

			if(!shortOptions.containsValue(responses[responseCount])){
				UI.print("Invalid repsonse: "+responses[responseCount]);
				return UI.askMultiWithOptions(question, fullOptions);
			}
			responseCount++;
		}
		return responses;
	}

	public static void exit(){
		UI.print("Exiting based on user input");
		System.exit(1);
	}
}
