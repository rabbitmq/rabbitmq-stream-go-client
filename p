[
    {
        "Id": "sha256:b92431c20744b254cc742fe40a5cf554391b99aa855db0c2d5b83c786a4935c7",
        "RepoTags": [
            "pivotalrabbitmq/stream-perf-test:latest"
        ],
        "RepoDigests": [
            "pivotalrabbitmq/stream-perf-test@sha256:566094791d8c82b4dd6f845bb9e234801e3d22280127674c37d715ea9dad3047"
        ],
        "Parent": "",
        "Comment": "",
        "Created": "2021-03-30T08:47:28.526187594Z",
        "Container": "0733b01fc9a5f75761b41210f1be9ce6b475ee906db3cccaca38f1d9c3521fca",
        "ContainerConfig": {
            "Hostname": "0733b01fc9a5",
            "Domainname": "",
            "User": "",
            "AttachStdin": false,
            "AttachStdout": false,
            "AttachStderr": false,
            "Tty": false,
            "OpenStdin": false,
            "StdinOnce": false,
            "Env": [
                "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                "LANG=en_US.UTF-8",
                "LANGUAGE=en_US:en",
                "LC_ALL=en_US.UTF-8",
                "JAVA_HOME=/usr/lib/jvm/java-1.11-openjdk/jre"
            ],
            "Cmd": [
                "/bin/sh",
                "-c",
                "#(nop) ",
                "ENTRYPOINT [\"java\" \"-Dio.netty.processId=1\" \"-jar\" \"stream-perf-test.jar\"]"
            ],
            "Image": "sha256:6049e92760af768dc7a8916eeda10086e529c409a3990c7f82b22a76d70dd67b",
            "Volumes": null,
            "WorkingDir": "/stream_perf_test",
            "Entrypoint": [
                "java",
                "-Dio.netty.processId=1",
                "-jar",
                "stream-perf-test.jar"
            ],
            "OnBuild": null,
            "Labels": {}
        },
        "DockerVersion": "19.03.12",
        "Author": "",
        "Config": {
            "Hostname": "",
            "Domainname": "",
            "User": "",
            "AttachStdin": false,
            "AttachStdout": false,
            "AttachStderr": false,
            "Tty": false,
            "OpenStdin": false,
            "StdinOnce": false,
            "Env": [
                "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                "LANG=en_US.UTF-8",
                "LANGUAGE=en_US:en",
                "LC_ALL=en_US.UTF-8",
                "JAVA_HOME=/usr/lib/jvm/java-1.11-openjdk/jre"
            ],
            "Cmd": null,
            "Image": "sha256:6049e92760af768dc7a8916eeda10086e529c409a3990c7f82b22a76d70dd67b",
            "Volumes": null,
            "WorkingDir": "/stream_perf_test",
            "Entrypoint": [
                "java",
                "-Dio.netty.processId=1",
                "-jar",
                "stream-perf-test.jar"
            ],
            "OnBuild": null,
            "Labels": null
        },
        "Architecture": "amd64",
        "Os": "linux",
        "Size": 148855530,
        "VirtualSize": 148855530,
        "GraphDriver": {
            "Data": {
                "LowerDir": "/var/lib/docker/overlay2/e5f1ca0f883211e2598f28a0991b0dce3d762c04530cb81eb20f1c79b684489b/diff:/var/lib/docker/overlay2/7f020166072f3053f2251f984f9f7d684266fe0170c6368b7288e20c2d79e344/diff:/var/lib/docker/overlay2/88d1f5ee865c1bd44bbd72248d25acb4ada48f134db02e8372b2e195269f8ee3/diff:/var/lib/docker/overlay2/ed3cf2d1a4dc43e051ac6d4b7b8c774b28b5828f391bc6a2d50bb1cef2a8dc68/diff:/var/lib/docker/overlay2/07164624e8bbd6d4910ec5dd29cbf5028cc56367892e9c76a06b167d17b71ed4/diff:/var/lib/docker/overlay2/95f622b12e7b6bb96a42c9471e293d96208386300e7cc04a9a3b1c52d6649acc/diff:/var/lib/docker/overlay2/3586c16059d2ac7d3d5115f91f2397a299d0b3f6c85c21c1fa17d46f273a96c5/diff:/var/lib/docker/overlay2/ae88e2732d66606f7e678a727e5084cfc7cf24e672f346daf926cee30399b23f/diff:/var/lib/docker/overlay2/8b588e93ac4800a860b1c1a8c12149b88cadecc4262f1bc4ace95e4ff190546a/diff",
                "MergedDir": "/var/lib/docker/overlay2/8076bcb5977d869e0303c92354f0355be3d31b2b384523ca1079f3017ea76e94/merged",
                "UpperDir": "/var/lib/docker/overlay2/8076bcb5977d869e0303c92354f0355be3d31b2b384523ca1079f3017ea76e94/diff",
                "WorkDir": "/var/lib/docker/overlay2/8076bcb5977d869e0303c92354f0355be3d31b2b384523ca1079f3017ea76e94/work"
            },
            "Name": "overlay2"
        },
        "RootFS": {
            "Type": "layers",
            "Layers": [
                "sha256:0c78fac124da333b91fb282abf77d9421973e4a3289666bd439df08cbedfb9d6",
                "sha256:cba97cc5811c5dbdc575384d01210ab6ba3c0ff972eea20a8b7546c5767c4edb",
                "sha256:d4dfaa212623040dfb425924a30450e168a626c98b74fa2eeac360e0ceb89be1",
                "sha256:35555555d73151cb326be11ef541b99bf01e8f86d7a61043023b5356d39f7a96",
                "sha256:d1e6e48dfe0b4c860062913ec8117a7c459ea7e0e379d87b6fec563e26b16061",
                "sha256:40b884eea85ea3f0ac0d2cb6c7104444bd124aedd8580f107cefc8ea902a5fe6",
                "sha256:ed5e9da9d1f6f2a2589d66a491ea6b82b5df82dd19b4059e5d4a99125a25fe13",
                "sha256:790644eb1070f2cab69f40207015cbfcc9f81486078d3c0f7221f108019eda56",
                "sha256:97a9bce9ad58ba64aea439b169fc5f2dc4b28f74dfc6fbee0e75df817e6b4099",
                "sha256:13e2ca1382d7da930d3daf7997082aaba93fde6909e227cb9870df4980033ee8"
            ]
        },
        "Metadata": {
            "LastTagTime": "0001-01-01T00:00:00Z"
        }
    }
]
