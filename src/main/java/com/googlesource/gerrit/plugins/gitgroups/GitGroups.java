// Copyright (C) 2012 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.googlesource.gerrit.plugins.gitgroups;

import static com.google.common.base.Preconditions.checkArgument;
import static org.eclipse.jgit.lib.Constants.HEAD;
import static org.eclipse.jgit.lib.Constants.R_REFS;
import static org.eclipse.jgit.lib.Constants.R_HEADS;

import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gerrit.common.data.GroupDescription;
import com.google.gerrit.common.data.GroupReference;
import com.google.gerrit.reviewdb.client.AccountGroup;
import com.google.gerrit.reviewdb.client.Project;
import com.google.gerrit.server.IdentifiedUser;
import com.google.gerrit.server.account.GroupBackend;
import com.google.gerrit.server.account.GroupMembership;
import com.google.gerrit.server.git.GitRepositoryManager;
import com.google.gerrit.server.project.NoSuchProjectException;
import com.google.gerrit.server.project.ProjectCache;
import com.google.gerrit.server.project.ProjectControl;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;

import org.eclipse.jgit.errors.RepositoryNotFoundException;
import org.eclipse.jgit.lib.ObjectReader;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.jgit.treewalk.filter.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@Singleton
class GitGroups implements GroupBackend {
  static final Logger log = LoggerFactory.getLogger(GitGroups.class);
  static final String MEMBERSHIP_CACHE = "membership";
  static final String UUID_PREFIX = "git:";
  private static final String NAME_PREFIX = "git/";

  private final ProjectControl.Factory projectControlFactory;
  private final ProjectCache projects;
  private final GitRepositoryManager manager;
  private final LoadingCache<AccountGroup.UUID, MemberList> membershipCache;
  private static final int MAX = 10;

  @Inject
  GitGroups(
      ProjectControl.Factory projectControlFactory,
      ProjectCache projects,
      GitRepositoryManager manager,
      @Named(MEMBERSHIP_CACHE) LoadingCache<AccountGroup.UUID, MemberList> membership) {
    this.projectControlFactory = projectControlFactory;
    this.projects = projects;
    this.manager = manager;
    this.membershipCache = membership;
  }

  @Override
  public boolean handles(AccountGroup.UUID uuid) {
    return uuid.get().startsWith(UUID_PREFIX);
  }

  @Override
  public GroupDescription.Basic get(final AccountGroup.UUID uuid) {
    checkArgument(handles(uuid), "GitGroups does not handle %s", uuid.get());
    try {
      membershipCache.get(uuid);
    } catch (ExecutionException e) {
      return null;
    }

    final String name = nameOf(uuid);
    return new GroupDescription.Basic() {
      @Override
      public AccountGroup.UUID getGroupUUID() {
        return uuid;
      }

      @Override
      public String getName() {
        return name;
      }

      @Override
      public boolean isVisibleToAll() {
        return false;
      }
    };
  }

  private static String nameOf(AccountGroup.UUID uuid) {
    return NAME_PREFIX + uuid.get().substring(UUID_PREFIX.length());
  }

  @Override
  public Collection<GroupReference> suggest(String name) {
    if (!name.startsWith(NAME_PREFIX) || name.equals(NAME_PREFIX)) {
      return Collections.emptyList();
    }

    String[] p = name.substring(NAME_PREFIX.length()).split(":");
    if (p.length == 1) {
      return suggestProject(p[0]);
    } else if (p.length == 2) {
      return suggestBranch(p[0], p[1]);
    } else if (p.length == 3) {
      return suggestFile(p[0], p[1], p[2]);
    } else {
      return Collections.emptySet();
    }
  }

  private Collection<GroupReference> suggestProject(String pfx) {
    List<GroupReference> matches = Lists.newArrayListWithCapacity(MAX);
    for (Project.NameKey project : projects.byName(pfx)) {
      try {
        projectControlFactory.validateFor(project);
      } catch (NoSuchProjectException e) {
        continue;
      }

      AccountGroup.UUID uuid =
          new AccountGroup.UUID(UUID_PREFIX + project.get() + ":");
      matches.add(new GroupReference(uuid, nameOf(uuid)));
      if (matches.size() == MAX) {
        break;
      }
    }
    return matches;
  }

  private Collection<GroupReference> suggestBranch(
      String projectName,
      String refPrefix) {
    Map<String, Ref> refs;
    ProjectControl ctl;
    try {
      Project.NameKey project = new Project.NameKey(projectName);
      ctl = projectControlFactory.validateFor(project);
      Repository git = manager.openRepository(project);
      try {
        refs = git.getAllRefs();
      } finally {
        git.close();
      }
    } catch (NoSuchProjectException e) {
      return Collections.emptyList();
    } catch (RepositoryNotFoundException e) {
      return Collections.emptyList();
    } catch (IOException e) {
      log.warn("Cannot read " + projectName, e);
      return Collections.emptyList();
    }

    List<GroupReference> matches = Lists.newArrayListWithCapacity(MAX);
    if (refPrefix.startsWith(R_REFS)) {
      for (Ref ref : refs.values()) {
        if (!ref.getName().startsWith("refs/changes/")
            && ref.getName().startsWith(refPrefix)
            && ref.getObjectId() != null
            && ctl.controlForRef(ref.getLeaf().getName()).isVisible()) {
          AccountGroup.UUID uuid = new AccountGroup.UUID(UUID_PREFIX
              + projectName + ":"
              + ref.getName() + ":");
          matches.add(new GroupReference(uuid, nameOf(uuid)));
          if (matches.size() == MAX) {
            break;
          }
        }
      }
    } else {
      for (Ref ref : refs.values()) {
        if (matchPrefix(ref, refPrefix)
            && ref.getObjectId() != null
            && ctl.controlForRef(ref.getLeaf().getName()).isVisible()) {
          AccountGroup.UUID uuid = new AccountGroup.UUID(UUID_PREFIX
              + projectName + ":"
              + ref.getName().substring(R_HEADS.length()) + ":");
          matches.add(new GroupReference(uuid, nameOf(uuid)));
          if (matches.size() == MAX) {
            break;
          }
        }
      }
    }
    return matches;
  }

  private Collection<GroupReference> suggestFile(
      String projectName,
      String refName,
      String filePrefix) {
    try {
      List<GroupReference> matches = Lists.newArrayListWithExpectedSize(MAX);
      Project.NameKey project = new Project.NameKey(projectName);
      ProjectControl ctl = projectControlFactory.validateFor(project);
      Repository git = manager.openRepository(project);
      try {
        Ref ref = git.getRef(refName);
        if (ref == null
            || ref.getObjectId() == null
            || !ctl.controlForRef(ref.getLeaf().getName()).isVisible()) {
          return Collections.emptyList();
        }

        ObjectReader reader = git.newObjectReader();
        try {
          RevWalk rw = new RevWalk(reader);
          TreeWalk tw = new TreeWalk(reader);
          tw.addTree(rw.lookupTree(ref.getObjectId()));
          int s = filePrefix.lastIndexOf('/');
          if (s >= 0) {
            tw.setFilter(PathFilter.create(filePrefix.substring(0, s)));
          }
          while (tw.next() && matches.size() < MAX) {
            String p = tw.getPathString();
            if (p.startsWith(filePrefix)) {
              AccountGroup.UUID uuid = new AccountGroup.UUID(UUID_PREFIX
                  + projectName + ":"
                  + refName + ":"
                  + p);
              matches.add(new GroupReference(uuid, nameOf(uuid)));
            }
          }
          return matches;
        } finally {
          reader.release();
        }
      } finally {
        git.close();
      }
    } catch (NoSuchProjectException e) {
      return Collections.emptyList();
    } catch (RepositoryNotFoundException e) {
      return Collections.emptyList();
    } catch (IOException e) {
      log.warn("Cannot read " + projectName + " branch " + refName, e);
      return Collections.emptyList();
    }
  }

  private static boolean matchPrefix(Ref ref, String refPrefix) {
    if (ref.getName().equals(HEAD)) {
      return ref.getName().startsWith(refPrefix);
    } else if (ref.getName().startsWith(R_HEADS)) {
      return ref.getName().substring(R_HEADS.length()).startsWith(refPrefix);
    }
    return false;
  }

  @Override
  public GroupMembership membershipsOf(IdentifiedUser user) {
    final String username = user.state().getUserName();
    final Set<String> emails = user.state().getEmailAddresses();
    final Set<AccountGroup.UUID> invalid = Sets.newHashSet();
    return new GroupMembership() {
      @Override
      public boolean contains(AccountGroup.UUID uuid) {
        try {
          return membershipCache.get(uuid).contains(username, emails);
        } catch (ExecutionException e) {
          if (invalid.add(uuid)) {
            Throwable c = e.getCause();
            if (c instanceof InvalidGitGroup) {
              log.warn(c.getMessage(), c.getCause());
            } else {
              log.warn("Cannot read group " + uuid.get(), e);
            }
          }
          return false;
        }
      }

      @Override
      public boolean containsAnyOf(Iterable<AccountGroup.UUID> groupIds) {
        int missing = 0;
        for (AccountGroup.UUID uuid : groupIds) {
          MemberList list = membershipCache.getIfPresent(uuid);
          if (list == null) {
            missing++;
          } else if (list.contains(username, emails)) {
            return true;
          }
        }
        if (missing > 0) {
          for (AccountGroup.UUID uuid : groupIds) {
            if (contains(uuid)) {
              return true;
            }
          }
        }
        return false;
      }

      @Override
      public Set<AccountGroup.UUID> getKnownGroups() {
        return Collections.emptySet(); // Optional, return empty set.
      }
    };
  }
}
