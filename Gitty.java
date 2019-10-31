package jutil;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.json.simple.parser.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jgit.*;
import org.eclipse.jgit.api.CloneCommand;
import org.eclipse.jgit.api.CommitCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.ListBranchCommand;
import org.eclipse.jgit.api.CreateBranchCommand;
import org.eclipse.jgit.api.DeleteBranchCommand;
import org.eclipse.jgit.api.StatusCommand;
import org.eclipse.jgit.api.DiffCommand;
import org.eclipse.jgit.api.AddCommand;
import org.eclipse.jgit.api.CheckoutCommand;
import org.eclipse.jgit.api.errors.AbortedByHookException;
import org.eclipse.jgit.api.errors.CannotDeleteCurrentBranchException;
import org.eclipse.jgit.api.errors.ConcurrentRefUpdateException;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.InvalidRefNameException;
import org.eclipse.jgit.api.errors.NoFilepatternException;
import org.eclipse.jgit.api.errors.NoHeadException;
import org.eclipse.jgit.api.errors.NoMessageException;
import org.eclipse.jgit.api.errors.NotMergedException;
import org.eclipse.jgit.api.errors.RefAlreadyExistsException;
import org.eclipse.jgit.api.errors.RefNotFoundException;
import org.eclipse.jgit.api.errors.UnmergedPathsException;
import org.eclipse.jgit.api.errors.WrongRepositoryStateException;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.dircache.DirCache;
import org.eclipse.jgit.errors.NoWorkTreeException;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;
import org.eclipse.jgit.treewalk.filter.TreeFilter;
import org.eclipse.jgit.api.Status;

public class Gitty {
	String email;
	String githubUser;
	String githubPass;
	String currentRepo; // assume it is always a .git dir
	String headBranch;
	List<Map<String,String>> tickets;
	Map<String, Map<String, String>> repoData = new HashMap<String, Map<String, String>>();

	Config myConfig;
	Crypto myCrypto;
	CredentialsProvider myCredsProvider;
	FileRepositoryBuilder builder;
	File repoDir;
	Repository repository;
	Git git;
	ArrayList<String> remoteBranches;
	ArrayList<String> localBranches;
	ObjectId lastCommitId;
	String ticketId;
	Ticket2 myTicket;

	public static void main(String[] args){
		Gitty myself = new Gitty();
		myself.tools();
	}

  public Gitty(){
  	this.myConfig = new Config();
  	this.myCrypto = new Crypto();
  	this.myTicket = new Ticket2();
  	this.builder = new FileRepositoryBuilder();

		this.githubUser = myConfig.getUserSetting("git-settings.github-creds.username");
		this.githubPass = myCrypto.getConfig("github-password");
		this.myCredsProvider = new UsernamePasswordCredentialsProvider(this.githubUser, this.githubPass);
    }

	public void run(){

		String options[] = {
				"status: get Current git Status",
				"log: Run git log command",
				"checkout: check out a specfic branch",
				"add: add files to the staging area",
				"commit: commit changes with a properly formatted message",
				"info: Current Status Information",
				"debug: Help figure out git issues",
				"merger: Help with merging files",
				"training: Load interactive training guide",
				"clone: Clone an existing repository"
			};

		String cmd =  UI.askWithOptions("What would you like to do?", options);

		switch(cmd) {
		case "log":

			FileRepositoryBuilder builder = new FileRepositoryBuilder();

			String repoPath = "path\\to\\.git";
			File repoDir = new File(repoPath);
			Repository repository;
			try {
				repository = builder.setGitDir(repoDir).readEnvironment().findGitDir().build();
				Git git = new Git(repository);
				Iterable<RevCommit> log = git.log().call();
				for(RevCommit commitItem : log){
					Run.print("Commiter Name: "+commitItem.getCommitterIdent().getName());
					Run.print("Commiter Email: "+commitItem.getCommitterIdent().getEmailAddress());
					Run.print("Time: "+commitItem.getCommitTime());
					Run.print("Message: "+commitItem.getFullMessage());
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			break;
		case "clone":
			this.runCloneWizard();
			break;
		default:
			Debug.pr("Unknown git command: "+cmd);
		}
	}

    public void setRepoData() {
    	Debug.pr("Gitty.setRepoData()");
    	List<String> clientList = this.myConfig.getClientList();
    }

    public String[] getRepoOptions(){
    	List<String> tmpList = new LinkedList<String>();
        List<String> keys = Util.getSortedListFromSet(this.repoData.keySet());
        Iterator<String> it = keys.iterator();

        while (it.hasNext()) {
            String key = it.next();
            String name = this.repoData.get(key).get("name");
            tmpList.add(key+": "+ name);
        }

    	return Util.getArrayFromList(tmpList);
    }

	public void setCurrentRepo(String currentRepo){
		try {
			this.currentRepo = currentRepo;
			this.repoDir = new File(this.currentRepo);
			this.repository = this.builder.setGitDir(this.repoDir).readEnvironment().findGitDir().build();
			this.headBranch = this.repository.getBranch();
			this.git = new Git(this.repository);

			this.remoteBranches = this.getRemoteBranches();
			this.localBranches = this.getLocalBranches();
			this.lastCommitId = this.repository.resolve(Constants.HEAD);
		} catch (Exception e) {
			Debug.pr("An Exception occurred!");
			Debug.pr(e.getMessage());
			e.printStackTrace();
		}
	}

	private void printHeader(){
		UI.print("Current Repo: "+this.currentRepo+ " - Checked out to : "+this.headBranch);
	}

	public void tools(){
		String[] repoList = this.getReposList();
		// add option to the end
		repoList = Util.appendStringToArray(repoList, "create-new: Launch the git clone wizard to create the repo you need.");

		String selectedRepo = UI.askWithOptions("What repo would you like to work on?", repoList);

		if(selectedRepo.equals("create-new")){
			this.runCloneWizard();
			// re-run tools()
			this.tools();
		}

		this.setCurrentRepo(this.myConfig.getUserSetting("file-system.git-repos")+"\\"+selectedRepo+"\\.git");
		this.loadGitStart();
	}

	public void loadGitStart(){
		this.printHeader();

		String options[] = {
				"branches: See all branches for this repo",
				"checkout: check out a specfic branch",
				"status: get Current git Status",
				"log: Run git log command",
				"diff: Run git diff command",
				"add: add files to the staging area",
				"commit: commit changes with a properly formatted message",
			};

		String cmd =  UI.askWithOptions("What would you like to do?", options);

		switch(cmd) {
		case "switch":
			this.tools();
			break;
		case "log":
			try {
				Iterable<RevCommit> log = this.git.log().call();
				for(RevCommit commitItem : log){
					Run.print("Commiter Name: "+commitItem.getCommitterIdent().getName());
					Run.print("Commiter Email: "+commitItem.getCommitterIdent().getEmailAddress());
					Run.print("Time: "+commitItem.getCommitTime());
					Run.print("Message: "+commitItem.getFullMessage());
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			break;
		case "branches":
			//get a list of existing branches to check out
			UI.printBar();
			UI.print("Local Branches:");
			for(String branch : this.localBranches)
				UI.print(branch);

			UI.printBar();
			UI.print("Remote Branches:");
			for(String branch : this.remoteBranches)
				UI.print(branch);
			UI.printBar();

			String subOptions[] = {
				"create-new : Launch the create new branch wizard.",
				"delete-local : Delete a local branch",
				"back : Go back to git tools home screen",
			};

			String action =  UI.askWithOptions("What would you like to do?", subOptions);

			switch(action){
			case "create-new":
				this.runCreateNewBranchWizard();
				break;
			case "delete-local":
				this.runDeleteLocalBranchWizard();
				break;
			case "back":
				this.loadGitStart();
				break;
			}
			break;
		case "checkout":
			//get a list of existing branches to check out
			localBranches = this.getLocalBranches();

			String branch =  UI.askWithOptions("What local branch would you like to check out?", localBranches);

			if(branch.equals("create-new"))
				this.runCreateNewBranchWizard();
			else
				this.checkoutBranch(branch);
			break;
		case "diff":
			List<String> uncommitedFiles = getDiffFiles("uncommitted");
			String[] diffFiles =  UI.askMultiWithOptions("What files would you like to run a diff against?", uncommitedFiles);

			String diffCmd = "git diff "+StringUtils.join(diffFiles, " ");
			Debug.expose(diffCmd);
			ArrayList<String> output = Run.execute(diffCmd);
			Debug.expose(output);
			break;
		case "add":
			// make sure the user isnt on master
			if(this.headBranch.equals("master")){

				UI.printClear("You current have the master branch checked out. You cannot make changes directly to master.");
			}
			else{
				AddCommand addCmd = this.git.add();
				uncommitedFiles = getDiffFiles("uncommitted");
				String[] filesToAdd = UI.askMultiWithOptions("What files would you like to add?", uncommitedFiles);
				for(String file : filesToAdd){
					File fileObj = new File(file);
					// todo - figure out why this wont work
					addCmd.addFilepattern(fileObj.getAbsolutePath());
				}
				try {
					DirCache dirCache = addCmd.call();
					Debug.expose(dirCache);
				} catch (Exception e) {
					Debug.pr(e.getMessage());
					e.printStackTrace();
				}
			}

			break;
		case "commit":
			// make sure the user isnt on master
			if(this.headBranch.equals("master")){

				UI.printClear("You current have the master branch checked out. You cannot make changes directly to master.");
				break;
			}
			// todo - get added and deleted files
			List<String> stagedFiles = getDiffFiles("changed");

			UI.print("The following files are staged for commit:");
			for(String file : stagedFiles)
				UI.print(file);

			if(UI.askBoolean("Are you sure you want to commit these files?")){
				String commitMsg = this.runCommitMsgWizard();
				CommitCommand commitCmd = this.git.commit();
				commitCmd.setMessage(commitMsg);
				try {

					String ticketMsg = commitMsg + "\n "+this.getLastCommitUrl();

					this.myTicket.addCommentToIssue(this.ticketId, ticketMsg);
				} catch (Exception e) {
					Debug.pr(e.getMessage());
					e.printStackTrace();
				}
			}
			break;
		case "status":
			try {
				Status status = this.git.status().call();
				List<String> added = this.getDiffFiles("added");
	            List<String> uncommittedChanges = this.getDiffFiles("uncommitted");
	            List<String> untracked = this.getDiffFiles("untracked");

	            for(String add : added)
	                UI.print("Added: " + add);

	            for(String uncommitted : uncommittedChanges)
	            	UI.print("Uncommitted: " + uncommitted);


			} catch (Exception e) {
				Debug.pr("An Exception ocurred");
				Debug.pr(e.getMessage());
				e.printStackTrace();
			}
			break;
		default:
			Debug.pr("Unknown git command: "+cmd);
		}

		this.loadGitStart();
	}

	private String runCommitMsgWizard(){
		String msg = "";

		this.ticketId = UI.ask("What ticket is this for?");

		String subject;
		Integer subjectCharLimit = 50;
		while(true){
			Boolean passAllTests = true;
			subject = UI.ask("Please enter a brief desription of this change. (limit to "+subjectCharLimit.toString()+" characters)");
			if(subject.length() > subjectCharLimit){
				UI.print("Too many characters.");
				passAllTests = false;
			}

			String firstChar = subject.substring(0, 1);

			if(!firstChar.equals(firstChar.toUpperCase())){
				UI.print("Please start with a capital letter");
				passAllTests = false;
			}

			if(passAllTests)
				break;
		}

		String description = UI.ask("If you have any more details about the change please enter them here.");

		if(UI.askBoolean("Can this be tested in a browser?"))
			description+= "\n * "+	UI.ask("Please enter a testing URL.");

		if(UI.askBoolean("Does this change require a server reboot?"))
			description+= "\n * Server reboot required.";

		if(UI.askBoolean("Does any SQL have to be run along with this change?"))
			description+= "\n * "+	UI.ask("Please provide instructions for running the SQL.");

		msg = subject;

		if(!this.ticketId.equals("") || !description.equals(""))
		{
			msg+= "\n";
			if(!this.ticketId.equals(""))
				msg+= "\n * Ticket: "+this.ticketId;
			if(!description.equals(""))
				msg+= "\n" + description;
		}

		Debug.pr(msg);

		return msg;
	}

	public List<String> getDiffFiles(String type){
		List<String> ret = null;
		Status status;
		try {
			status = this.git.status().call();
			switch(type){
			case "added":
				ret = Util.getSortedListFromSet(status.getAdded());
				break;
			case "uncommitted":
				ret = Util.getSortedListFromSet(status.getUncommittedChanges());
				break;
			case "untracked":
				ret = Util.getSortedListFromSet(status.getUntracked());
				break;
			case "changed":
				ret = Util.getSortedListFromSet(status.getChanged());
				break;
			}

		} catch (Exception e) {
			Debug.pr("An Exception ocurred");
			Debug.pr(e.getMessage());
			e.printStackTrace();
		}
		return ret;
	}

	public void checkoutBranch(String branch){
		try{
			CheckoutCommand tmpCommand = this.git.checkout();
			tmpCommand.setName(branch);
			tmpCommand.call();

			this.headBranch = branch;
		}
		catch(Exception e){
			Debug.pr(e.getMessage());
		}
	}

	public ArrayList<String> getLocalBranches(){
		ArrayList<String> localBranches = new ArrayList<>();
		ListBranchCommand branchCommand = this.git.branchList();
		try {
			List<Ref> branchRefs = branchCommand.call();
			for(Ref branchRef : branchRefs){
				localBranches.add(branchRef.getName().replace("refs/heads/", ""));
			}
		}catch(Exception e){
			Debug.pr(e.getMessage());
		}

		return localBranches;
	}

	public ArrayList<String> getRemoteBranches(){
		ArrayList<String> remoteBranches = new ArrayList<>();
		try {
			ListBranchCommand cmd = this.git.branchList();
			cmd.setListMode(ListBranchCommand.ListMode.REMOTE);
			List<Ref> refList = cmd.call();
			for(Ref ref : refList){
				String branchName = ref.getName().replace("refs/remotes/origin/", "");
				if(!branchName.equals("HEAD"))
					remoteBranches.add(ref.getName().replace("refs/remotes/origin/", ""));
			}
		}catch(Exception e){
			Debug.pr(e.getMessage());
		}
		return remoteBranches;
	}

	public void runCreateNewBranchWizard(){
		CreateBranchCommand cmd = this.git.branchCreate();
		ArrayList<String> remoteBranches = this.getRemoteBranches();
		remoteBranches.add("create-new: Create a new branch not in remote repo.");
		String branchName = UI.askWithOptions("What remote branch would you like to check out locally?", remoteBranches);

		if(branchName.equals("create-new")){
			branchName = UI.ask("What would you like to name the new branch?");
			List<String> tmpOptions = (List<String>) this.localBranches.clone();
			tmpOptions.add("not-shown");
			String baseBranch = UI.askWithOptions("What local branch would you like to base this branch off of?", tmpOptions);

			if(baseBranch.equals("not-shown"))
				Debug.die("not coded yet");

			this.checkoutBranch(baseBranch);
		}
		else{
			cmd.setName(branchName);
		}

		try {
			Ref branchCommit =  cmd.call();
		} catch (Exception e) {
			Debug.pr(e.getMessage());
			e.printStackTrace();
		}

		// refresh local branches
		this.localBranches = this.getLocalBranches();
	}

	public void runDeleteLocalBranchWizard(){
		String deleteBranch = UI.askWithOptions("What local branch would you like to delete?", this.localBranches);

		if(UI.askBoolean("Are you sure you want to delete local branch: "+ deleteBranch+"? This cannot be undone."))
			this.deleteLocalBranch(deleteBranch);

		this.loadGitStart();
	}

	public void deleteLocalBranch(String branchName){
		DeleteBranchCommand	cmd = this.git.branchDelete();
		try {
			cmd.setBranchNames(branchName).call();
		} catch (NotMergedException e) {
			e.printStackTrace();
		} catch (CannotDeleteCurrentBranchException e) {
			e.printStackTrace();
		} catch (GitAPIException e) {
			e.printStackTrace();
		}

		// refresh local branches
		this.localBranches = this.getLocalBranches();
	}


	public void runCloneWizard() {
		List<String> clientList = this.myConfig.getClientList();

		String clientId =  UI.askWithSearchableOptions("What repository would you like to clone?", clientList);
		String repoId = this.myConfig.getClientSetting(clientId, "git-repo-id");

		try{

			// determine where the user wants to store the repo
			String defaultParentDir = this.myConfig.getUserSetting("file-system.git-repos");
			String parentDir;
			UI.print("Your default parent git directory is: " + defaultParentDir);

			if(UI.askBoolean("Would you like to store this repo there?"))
				parentDir = defaultParentDir;
			else
				parentDir = UI.promptForDirectory("Where would you like to store this repo?");

			// make sure this directory exists
			File tmpFile = new File(parentDir);
			if(!tmpFile.exists()) {
				UI.print("The parent directory: "+parentDir+" does not exist. Starting over...");
				this.runCloneWizard();
			}

			String repoDirName;
			if(UI.askBoolean("Would you like to use the default repo name of: "+repoId))
				repoDirName = repoId;
			else
				repoDirName = UI.ask("What would you like the name the directory that stores this repository");

			// make sure this directory doesnt already exist
			tmpFile = new File(parentDir+"\\"+repoDirName);
			if(tmpFile.exists()) {
				UI.print("The parent directory: "+tmpFile.getPath()+" already exists. Starting over...");
				this.runCloneWizard();
			}

			UI.print("Creating new directory...");
			tmpFile.mkdir();

			UI.print("Cloning repo... (this may take several minutes)");

			CloneCommand myCloneCommand = Git.cloneRepository();
			Debug.pr("https://brg.git.beanstalkapp.com/"+repoId+".git");
			myCloneCommand.setURI("https://brg.git.beanstalkapp.com/"+repoId+".git");
			myCloneCommand.setDirectory(tmpFile);
			myCloneCommand.setCredentialsProvider(this.myCredsProvider);
			myCloneCommand.call();

			Run.print("Clone complete.");
		}
		catch(Exception e) {
			Run.print("Exception Occurred!");
			Debug.expose(e);
			Debug.die();
		}
	}

    public void log() {
    	String cmd = "git --git-dir=/mnt/c/jutil/workspace/src/jutil/git/dev/test/.git log";
    	List<String> ret = myRun.execute(cmd);
    }

    public void commit(){
    	myRun.print("What ticket is this commit for?");
    	String ticketId = "myRun.buf.readLine()";
    	Map<String,String> ticketData = this.getTicketData(ticketId); //@todo - make ticket its own class
    	if(ticketData.isEmpty()) {
    		myRun.print("There is no data for ticket: "+ticketId);
    		this.commit();
    	}
    	// confirm with the user that this is the ticket they want to work no
    	myRun.print("Are you sure you want to commit for ticket: "+ticketData.get("title"));
    	String response = "myRun.buf.readLine()";
    	switch(response) {
    	case "n":
    	case "no":
    		myRun.print("OK restarting...");
    		this.commit();
    		return;
    		//break;
    	case "y":
    	case "yes":
    		// continue
    		break;
    	}

    	// verify that the branch matches
    	if(!ticketData.get("release").equals(this.headBranch)){
    		myRun.print("You cannot commit this ticket to this branch.");
    		myRun.print("This ticket is for release: "+ticketData.get("release"));
    		myRun.print("You current have this branch checked out: "+this.headBranch);
    		myRun.print("restarting...");
    		this.commit();
    		return;
    	}

    	// if you are still here the repo matches
    	// get basic description

    	myRun.print("Please provide a brief description of your changes. (limit 72 characters)");
    	String description = "myRun.buf.readLine()";

    	myRun.print("Does this change require and SQL executions?");
    	String sqlExecutions = "myRun.buf.readLine()";

    	myRun.print("Does this change require a reboot?");
    	String reboot = "myRun.buf.readLine()";

    	// at this point we can just format a nicely formatted commit message

    	String cmd = "git commit -m \""+description
    			+ "\n"
    			+ "Ticket ID: "+ticketId
    			+ "\n"
    			+ "Title: "+ticketData.get("title")
    			+ "\n"
    			+ "Release: "+ticketData.get("release");

    	myRun.execute(cmd);


    	//update ticket status
    }

   public String[] getReposList(){
    	ArrayList<String> repoList = new ArrayList<>();
    	// get a list of all the directories from the default git repo
    	String gitReposDir = myConfig.getUserSetting("file-system.git-repos");
    	File folder = new File(gitReposDir);
    	File[] listOfFiles = folder.listFiles();

    	String branchName = "";

    	for (int i = 0; i < listOfFiles.length; i++) {
    	  if (listOfFiles[i].isDirectory()) {

			FileRepositoryBuilder builder = new FileRepositoryBuilder();

			String repoPath = listOfFiles[i].getName()+"\\.git";
			File repoDir = new File(gitReposDir+"\\"+repoPath);
			Repository repository;
			try {
				repository = builder.setGitDir(repoDir).readEnvironment().findGitDir().build();
				branchName = repository.getBranch();
			}
			catch(Exception e){
				Debug.pr("An exception ocurred!");
				Debug.expose(e.getMessage());
			}
    	    repoList.add(listOfFiles[i].getName()+" : Currently checked out to : "+branchName);
    	  }
    	}

    	return repoList.toArray(new String[0]);
    }

    public Map<String,String> getTicketData(String ticketId){
    	Debug.pr("getTicketData("+ticketId+")");
    	Map<String,String> ret = new HashMap();

    	for(Map<String,String> ticket:  this.tickets) {

    		if(ticket.get("id").equals(ticketId))
    			return ticket;

    	}

    	return ret;
    }

    public void info() {
    	myRun.print("You are currently logged in as: "+this.beanstalkUser);
    	myRun.print("Your email is: "+this.email);
    	myRun.print("Your checked out repo is: "+this.headBranch);
    	myRun.print("Ticket List");
    	for(Map<String,String> ticket:  this.tickets) {
    		myRun.printBar();
    		Debug.expose(ticket);
    		myRun.printBar();
    	}
    }

    private String getSavedEmail() {
    	return "this.email";
    }

    public String initEmail() {
    	return "this.email";
    }
}
