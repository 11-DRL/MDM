import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { NotFound } from './NotFound';

function renderAt(pathname: string) {
  return render(
    <MemoryRouter initialEntries={[pathname]}>
      <NotFound />
    </MemoryRouter>,
  );
}

describe('NotFound', () => {
  it('renders 404 heading', () => {
    renderAt('/foo/bar');
    expect(screen.getByRole('heading', { name: /404/i })).toBeInTheDocument();
  });

  it('shows the pathname that was not found', () => {
    renderAt('/does-not-exist');
    expect(screen.getByText('/does-not-exist')).toBeInTheDocument();
  });

  it('renders a link back to the review queue', () => {
    renderAt('/typo');
    const link = screen.getByRole('link', { name: /Review Queue/i });
    expect(link).toHaveAttribute('href', '/queue');
  });
});
