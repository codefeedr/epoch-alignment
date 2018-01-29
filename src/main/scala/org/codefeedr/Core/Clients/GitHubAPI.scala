package org.codefeedr.Core.Clients

import com.typesafe.config.{Config, ConfigFactory}
import org.codefeedr.Core.Input.GitHubSource
import org.eclipse.egit.github.core.client.GitHubClient
import org.slf4j.{Logger, LoggerFactory}

/**
  * Wrapper class for setting up GitHubAPI connection.
  */
class GitHubAPI {

  //get the codefeedr configuration files
  private lazy val conf: Config = ConfigFactory.load()

  //Github API rate limit
  private val _rateLimit: Integer = 5000

  //waiting time between request so there are no conflicts with the rate limit
  private val _waitingTime = _rateLimit / 3600

  //the amount of events requested per poll
  private var _eventsPerPoll = 100 //maximum of 300 events per request TODO: Check this

  //initialize githubclient
  @transient
  private lazy val _client: GitHubClient = new GitHubClient

  //some getters
  def client = _client
  def rateLimit = _rateLimit
  def waitingTime = _waitingTime
  def eventsPerPoll = _eventsPerPoll

  /**
    * Set the OAuthToken of the GitHub API.
    */
  def SetOAuthToken() = {
    client.setOAuth2Token(conf.getString("codefeedr.input.github.apikey"))
  }

  //set the auth token
  SetOAuthToken()
}
