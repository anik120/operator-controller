---
title: Components
layout: default
nav_order: 3
---

v0 Operator-Lifecycle-Manager was built as a [mono repo](https://github.com/operator-framework/operator-lifecycle-manager), which lead to an increase in the complexity of maintaining the project as it grew over time. 

In the v1 rewrite of the project, the implementation is being split into various component projects: 


* [rukpak](https://github.com/operator-framework/rukpak): RukPak is a pluggable solution for the packaging and distribution of cloud-native content and supports advanced strategies for installation, updates, and policy. The project provides a content ecosystem for installing a variety of artifacts, such as Git repositories, Helm charts, OLM bundles, and more onto a Kubernetes cluster. These artifacts can then be managed, scaled, and upgraded in a safe way to enable powerful cluster extensions.
At its core, RukPak is a small set of APIs, packaged as Kubernetes CustomResourceDefinitions, and controllers that watch for those APIs. These APIs express what content is being installed on-cluster and how to create a running deployment of the content.


* [deppy](https://github.com/operator-framework/deppy): Deppy is a Kubernetes API that runs on- or off-cluster for resolving constraints over catalogs of RukPak bundles. Deppy is part of the next iteration of OLM and was first introduced here. The initial goal of the project is to remove the dependency manager from the Operator Lifecycle Manager (OLM) and make it its own generic component.


* [catalogD](https://github.com/operator-framework/catalogd): Catalogd is a Kubernetes extension that unpacks [file-based catalog (FBC)](https://olm.operatorframework.io/docs/reference/file-based-catalogs/#docs) content that is packaged and shipped in container images, for consumption by clients on-clusters (unpacking from other sources, like git repos, OCI artifacts etc, are in the roadmap for catalogD). As component of the Operator Lifecycle Manager (OLM) v1 microservices architecture, catalogD hosts metadata for Kubernetes extensions packaged by the authors of the extensions, as a result helping customers discover installable content.

* [operator-controller](https://github.com/operator-framework/operator-controller): Finally, operator-controller is the central component of OLM v1, that consumes all of the components above to extend Kubernetes to allows users to install, and manage the lifecycle of other extensions