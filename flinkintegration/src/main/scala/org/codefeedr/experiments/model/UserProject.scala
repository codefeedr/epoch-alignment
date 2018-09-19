package org.codefeedr.experiments.model

case class UserProject(userId: Long,
                       projectId: Long,
                       userLogin: String,
                       projectDescription: String)
