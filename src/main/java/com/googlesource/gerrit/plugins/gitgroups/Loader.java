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

import static com.googlesource.gerrit.plugins.gitgroups.GitGroups.log;
import static org.eclipse.jgit.lib.Constants.HEAD;
import static org.eclipse.jgit.lib.Constants.OBJ_BLOB;

import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gerrit.extensions.events.LifecycleListener;
import com.google.gerrit.reviewdb.client.Account;
import com.google.gerrit.reviewdb.client.AccountGroup;
import com.google.gerrit.reviewdb.client.Project;
import com.google.gerrit.server.git.GitRepositoryManager;
import com.google.gerrit.server.mail.Address;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectReader;
import org.eclipse.jgit.lib.ObjectStream;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.TreeWalk;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

/**
 * Reads membership lists from Git repositories.
 * <p>
 * Refresh for updated lists happens in the background on a single thread.
 */
@Singleton
class Loader extends CacheLoader<AccountGroup.UUID, MemberList>
    implements LifecycleListener {
  private final GitRepositoryManager mgr;
  private final UpdateListener updateListener;
  private volatile ExecutorService refreshExecutor;

  @Inject
  Loader(GitRepositoryManager mgr, UpdateListener updateListener) {
    this.mgr = mgr;
    this.updateListener = updateListener;
  }

  @Override
  public void start() {
    refreshExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat("GitGroups-Refresh-%d")
      .build());
  }

  @Override
  public void stop() {
    ExecutorService r = refreshExecutor;
    if (r != null && !r.isShutdown()) {
      r.shutdownNow();
    }
  }

  @Override
  public MemberList load(AccountGroup.UUID uuid) throws Exception {
    MemberList list = load(uuid, null);
    updateListener.add(uuid, list.project, list.refName);
    return list;
  }

  @Override
  public ListenableFuture<MemberList> reload(
      final AccountGroup.UUID uuid,
      final MemberList oldList) {
    ExecutorService executor = refreshExecutor;
    if (executor == null || executor.isShutdown() || executor.isTerminated()) {
      return Futures.immediateFuture(oldList);
    }
    try {
      final SettableFuture<MemberList> future = SettableFuture.create();
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            MemberList newList = load(uuid, oldList);
            if (!oldList.refName.equals(newList.refName)) {
              updateListener.remove(uuid, oldList.project, oldList.refName);
              updateListener.add(uuid, newList.project, newList.refName);
            }
            future.set(newList);
          } catch (InvalidGitGroup e) {
            log.warn(String.format(
                "Cannot reload %s; keeping prior version",
                uuid.get()), e);
            future.set(oldList);
          }
        }
      });
      return future;
    } catch (RejectedExecutionException e) {
      return Futures.immediateFuture(oldList);
    }
  }

  private MemberList load(AccountGroup.UUID uuid, MemberList old)
      throws InvalidGitGroup {
    // Either "git:project:file" or "git:project:branch:file".
    String[] p =
        uuid.get().substring(GitGroups.UUID_PREFIX.length()).split(":");
    if (p.length != 2 && p.length != 3) {
      throw new InvalidGitGroup("Invalid UUID format " + uuid.get());
    }

    Project.NameKey project = new Project.NameKey(p[0]);
    String branch = p.length == 3 ? p[1] : HEAD;
    String file = p.length == 3 ? p[2] : p[1];

    try {
      Repository git = mgr.openRepository(project);
      try {
        Ref ref = git.getRef(branch);
        if (ref == null || ref.getObjectId() == null) {
          throw new InvalidGitGroup("Branch does not exist for " + uuid.get());
        }
        ref = ref.getLeaf();

        ObjectId refId = ref.getObjectId();
        if (old != null && old.refObjectId.equals(refId)) {
          return old;
        }

        ObjectReader reader = git.newObjectReader();
        try {
          RevWalk rw = new RevWalk(reader);
          TreeWalk tw = TreeWalk.forPath(reader, file, rw.parseTree(refId));
          if (tw == null) {
            throw new InvalidGitGroup("File does not exist for " + uuid.get());
          }

          ObjectId fileId = tw.getObjectId(0);
          if (old != null && old.fileObjectId.equals(fileId)) {
            return old;
          }

          ObjectStream in = reader.open(fileId, OBJ_BLOB).openStream();
          try {
            if (in.getType() != OBJ_BLOB) {
              throw new InvalidGitGroup("Not a blob " + uuid.get());
            }

            Set<String> members = Sets.newHashSet();
            BufferedReader br = decode(in);
            String line;
            int lineNbr = 0;
            while ((line = br.readLine()) != null) {
              try {
                line = line.trim();
                lineNbr++;
                if (line.startsWith("#") || line.isEmpty()) {
                  // Skip comments and blank lines.
                } else if (line.matches(Account.USER_NAME_PATTERN)) {
                  members.add(line);
                } else if (line.indexOf('@') > 0) {
                  members.add(Address.parse(line).getEmail());
                } else {
                  throw new IllegalArgumentException();
                }
              } catch (IllegalArgumentException e) {
                log.warn(String.format(
                    "Line %d of %s:%s is invalid in commit %s",
                    lineNbr, branch, file, refId.name()));
              }
            }

            return new MemberList(
                uuid,
                project,
                ref.getName(),
                refId,
                fileId,
                ImmutableSet.copyOf(members));
          } finally {
            in.close();
          }
        } finally {
          reader.release();
        }
      } finally {
        git.close();
      }
    } catch (IOException e) {
      throw new InvalidGitGroup("Cannot read membership of " + uuid.get(), e);
    }
  }

  private static BufferedReader decode(ObjectStream stream)
      throws UnsupportedEncodingException {
    return new BufferedReader(new InputStreamReader(stream, "UTF-8"));
  }
}
