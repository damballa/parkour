require 'oryx'

task(:bootstrap)
task(:test) { sh "lein test" }
task(:package) { Oryx::package_lein_jar }
