package wikipedia

import java.io.File

object WikipediaData {

  private[wikipedia] def filePath = {
    new File(this.getClass.getClassLoader.getResource("wikipedia/wikipedia.dat").toURI).getPath
  }

  private[wikipedia] def parse(line: String): WikipediaArticle = {
    val subs = "</title><text>"
    val i = line.indexOf(subs)
    val title = line.substring(14, i)
    val text  = line.substring(i + subs.length, line.length-16)
    WikipediaArticle(title, text)
  }
}
