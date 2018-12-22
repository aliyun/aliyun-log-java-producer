.PHONY: deploy
deploy:
	mvn clean deploy -Dmaven.test.skip=true

.PHONY: release
clean:
	mvn -Darguments="-DskipTests" release:clean release:prepare release:perform





