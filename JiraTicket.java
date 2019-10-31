package jutil;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import de.*;
import de.micromata.jira.rest.JiraRestClient;
import de.micromata.jira.rest.client.ProjectClient;
import de.micromata.jira.rest.client.SystemClient;
import de.micromata.jira.rest.core.domain.ProjectBean;

import de.micromata.jira.rest.JiraRestClient;
import de.micromata.jira.rest.core.domain.*;
import de.micromata.jira.rest.core.domain.field.FieldBean;
import de.micromata.jira.rest.core.domain.update.FieldOperation;
import de.micromata.jira.rest.core.domain.update.IssueUpdate;
import de.micromata.jira.rest.core.domain.update.Operation;
import de.micromata.jira.rest.core.jql.EField;
import de.micromata.jira.rest.core.misc.JsonConstants;
import de.micromata.jira.rest.core.util.RestException;
import de.micromata.jira.rest.core.util.URIHelper;
import org.apache.commons.lang3.ArrayUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class JiraTicket {

	JiraRestClient jiraRestClient;

	public JiraTicket(){
		ExecutorService executorService = Executors.newFixedThreadPool(100);
		// ProxyHost proxy = new ProxyHost("proxy", 3128);
		URI uri = null;
		try {
			uri = new URI("https://brgsupport.atlassian.net");
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		jiraRestClient = new JiraRestClient(executorService);
		try {
		jiraRestClient.connect(uri, "test@test.com", "secret");

		ProjectClient projCli = jiraRestClient.getProjectClient();

		Future<ProjectBean> future = projCli.getProjectByKey("HARS");
	  final ProjectBean project = future.get();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

  public void addCommentToIssue(String ticketId, String comment) throws URISyntaxException, IOException, RestException {
    CommentBean commentBean = new CommentBean();
    commentBean.setBody(comment);
    boolean commentToIssue = jiraRestClient.getIssueClient().addCommentToIssue(ticketId, commentBean);
  }
}