/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.program;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.app.Id;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.weave.filesystem.Location;

/**
 * A delegation of {@link Program} interface.
 */
public abstract class ForwardingProgram implements Program {

  protected final Program delegate;

  protected ForwardingProgram(Program delegate) {
    this.delegate = delegate;
  }

  @Override
  public Class<?> getMainClass() throws ClassNotFoundException {
    return delegate.getMainClass();
  }

  @Override
  public Type getType() {
    return delegate.getType();
  }

  @Override
  public Id.Program getId() {
    return delegate.getId();
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public String getAccountId() {
    return delegate.getAccountId();
  }

  @Override
  public String getApplicationId() {
    return delegate.getApplicationId();
  }

  @Override
  public ApplicationSpecification getSpecification() {
    return delegate.getSpecification();
  }

  @Override
  public Location getJarLocation() {
    return delegate.getJarLocation();
  }

  @Override
  public ClassLoader getClassLoader() {
    return delegate.getClassLoader();
  }
}
