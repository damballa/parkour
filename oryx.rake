require 'oryx'

task(:bootstrap)
task(:test) { sh "lein test-all" }
task(:package) { Oryx::package_lein_jar }
