using System;
using System.Collections.Generic;
using System.Text;

namespace Apache.Arrow.Types
{
    public abstract class NestedType : ArrowType
    {
        public List<Field> Children { get; }

        public Field Child(int i) => Children[i];

        public int ChildrenNumber => Children?.Count ?? 0;

        protected NestedType(List<Field> children)
        {
            if (children == null || children.Count == 0)
            {
                throw new ArgumentNullException(nameof(children));
            }
            Children = children;
        }

        protected NestedType(Field child)
        {
            if (child == null)
            {
                throw new ArgumentNullException(nameof(child));
            }
            Children = new List<Field>{ child };
        }
    }
}
