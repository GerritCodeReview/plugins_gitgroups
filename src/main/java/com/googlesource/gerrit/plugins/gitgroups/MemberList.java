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

import com.google.common.collect.ImmutableSet;
import com.google.gerrit.reviewdb.client.AccountGroup;
import com.google.gerrit.reviewdb.client.Project;

import org.eclipse.jgit.lib.ObjectId;

import java.util.Set;

/**
 * Holds the list of usernames and email addresses that are in a group.
 * <p>
 * This list is cached in the server and updated internally whenever a Git
 * reference gets modified.
 */
class MemberList {
  static class Weigher implements
      com.google.common.cache.Weigher<AccountGroup.UUID, MemberList> {
    @Override
    public int weigh(AccountGroup.UUID uuid, MemberList list) {
      return list.weight();
    }
  }

  final AccountGroup.UUID uuid;
  final Project.NameKey project;
  final String refName;
  final ObjectId refObjectId;
  final ObjectId fileObjectId;
  final ImmutableSet<String> members;

  MemberList(AccountGroup.UUID uuid,
      Project.NameKey project,
      String refName,
      ObjectId refObjectId,
      ObjectId fileObjectId,
      ImmutableSet<String> members) {
    this.uuid = uuid;
    this.project = project;
    this.refName = refName;
    this.refObjectId = refObjectId;
    this.fileObjectId = fileObjectId;
    this.members = members;
  }

  boolean contains(String username, Set<String> emails) {
    if (username != null && members.contains(username)) {
      return true;
    }
    if (!emails.isEmpty()) {
      for (String addr : emails) {
        if (members.contains(addr)) {
          return true;
        }
      }
    }
    return false;
  }

  int weight() {
    int weight = 0;
    for (String m : members) {
      weight = m.length() * 2 + 32;
    }
    return weight;
  }

  @Override
  public String toString() {
    return uuid.get();
  }
}
