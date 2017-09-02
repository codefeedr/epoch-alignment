package org.codefeedr.Core.Engine.Query

import org.codefeedr.Core.Library.SubjectLibrary

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * Created by Niels on 31/07/2017.
  */
object StreamComposerFactory {
  def GetComposer(query: QueryTree): Future[StreamComposer] = {
    query match {
      case SubjectSource(subjectName) =>
        SubjectLibrary.AwaitTypeRegistration(subjectName).map(o => new SourceStreamComposer(o))
      case Join(left, right, keysLeft, keysRight, selectLeft, selectRight, alias) =>
        for {
          leftComposer <- GetComposer(left)
          rightComposer <- GetComposer(right)
          joinedType <- SubjectLibrary.GetOrCreateType(
            alias,
            () =>
              JoinQueryComposer.buildComposedType(leftComposer.GetExposedType(),
                                                  rightComposer.GetExposedType(),
                                                  selectLeft,
                                                  selectRight,
                                                  alias))
        } yield
          new JoinQueryComposer(leftComposer, rightComposer, joinedType, query.asInstanceOf[Join])
      case _ => throw new NotImplementedError("not implemented query subtree")
    }
  }
}
