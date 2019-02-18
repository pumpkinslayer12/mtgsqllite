# mtgsqllite
Command line tool to convert allsets.json file from mtg JSON to sqlite database. Tool accepts three arguments.
<ul>
  <li>sqllite database path</li>
  <li>json file path</li>
  <li>overide database boolean</li>
</ul>

Future plans:
<ul>
  <li>Pull JSON directory directly from mtg json</li>
  <li>Recursion based approach to parse json file. Idea is be able to add additional columns of data without having to care about what was added.</li>
  <li>Fix bug preventing unique constraint on sets table.</li>
  
</ul>
