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

import static com.googlesource.gerrit.plugins.gitgroups.GitGroups.MEMBERSHIP_CACHE;

import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gerrit.extensions.events.GitReferenceUpdatedListener;
import com.google.gerrit.reviewdb.client.AccountGroup;
import com.google.gerrit.reviewdb.client.Project;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Receives notification of updates in Git, refreshing member lists.
 * <p>
 * When Gerrit changes a Git branch that contains a member list, this
 * listener asks the cache to refresh the list.
 */
@Singleton
class UpdateListener implements GitReferenceUpdatedListener {
  private final Map<Project.NameKey, Map<String, Set<AccountGroup.UUID>>> live;
  private final LoadingCache<AccountGroup.UUID, MemberList> membership;

  @Inject
  UpdateListener(@Named(MEMBERSHIP_CACHE) LoadingCache<AccountGroup.UUID, MemberList> cache) {
    this.live = Maps.newHashMap();
    this.membership = cache;
  }

  @Override
  public synchronized void onGitReferenceUpdated(Event event) {
    Map<String, Set<AccountGroup.UUID>> p = live.get(event.getProjectName());
    if (p == null) {
      // No groups use this project, early return to save time.
      return;
    }

    for (Update u : event.getUpdates()) {
      Set<AccountGroup.UUID> r = p.get(u.getRefName());
      if (r != null) {
        Iterator<AccountGroup.UUID> i = r.iterator();
        while (i.hasNext()) {
          AccountGroup.UUID uuid = i.next();
          if (membership.getIfPresent(uuid) != null) {
            membership.refresh(uuid);
          } else {
            i.remove();
          }
        }
        if (r.isEmpty()) {
          p.remove(u.getRefName());
        }
      }
    }

    if (p.isEmpty()) {
      live.remove(event.getProjectName());
    }
  }

  synchronized void add(AccountGroup.UUID uuid,
      Project.NameKey project,
      String refName) {
    Map<String, Set<AccountGroup.UUID>> p = live.get(project);
    if (p == null) {
      p = Maps.newHashMap();
      live.put(project, p);
    }

    Set<AccountGroup.UUID> r = p.get(refName);
    if (r == null) {
      r = Sets.newHashSet();
      p.put(refName, r);
    }

    r.add(uuid);
  }

  synchronized void remove(AccountGroup.UUID uuid,
      Project.NameKey project,
      String refName) {
    Map<String, Set<AccountGroup.UUID>> p = live.get(project);
    if (p == null) {
      return;
    }

    Set<AccountGroup.UUID> r = p.get(refName);
    if (r == null) {
      return;
    }

    r.remove(uuid);

    if (r.isEmpty()) {
      p.remove(refName);
      if (p.isEmpty()) {
        live.remove(project);
      }
    }
  }
}
