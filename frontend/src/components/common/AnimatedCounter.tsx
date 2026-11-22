import React, { useEffect, useState, useRef } from 'react';

interface AnimatedCounterProps {
  value: number;
  duration?: number;
  decimals?: number;
  prefix?: string;
  suffix?: string;
  className?: string;
  formatter?: (val: number) => string;
}

/**
 * Optical Refraction Easing: cubic-bezier(0.16, 1, 0.24, 1)
 */
function easeOutRefraction(t: number): number {
  // Approximate cubic-bezier(0.16, 1, 0.24, 1)
  return 1 - Math.pow(1 - t, 3.5);
}

export const AnimatedCounter: React.FC<AnimatedCounterProps> = ({
  value,
  duration = 1400,
  decimals = 0,
  prefix = '',
  suffix = '',
  className = '',
  formatter,
}) => {
  const [displayValue, setDisplayValue] = useState<number>(0);
  const startValRef = useRef<number>(0);
  const targetValRef = useRef<number>(value);
  const startTimeRef = useRef<number | null>(null);
  const rafRef = useRef<number | null>(null);

  useEffect(() => {
    startValRef.current = displayValue;
    targetValRef.current = value;
    startTimeRef.current = null;

    const animate = (timestamp: number) => {
      if (!startTimeRef.current) startTimeRef.current = timestamp;
      const progress = Math.min((timestamp - startTimeRef.current) / duration, 1);
      const eased = easeOutRefraction(progress);
      const current = startValRef.current + (targetValRef.current - startValRef.current) * eased;
      setDisplayValue(current);

      if (progress < 1) {
        rafRef.current = requestAnimationFrame(animate);
      } else {
        setDisplayValue(targetValRef.current);
      }
    };

    rafRef.current = requestAnimationFrame(animate);

    return () => {
      if (rafRef.current) cancelAnimationFrame(rafRef.current);
    };
  }, [value, duration]);

  const formatted = formatter
    ? formatter(displayValue)
    : displayValue.toLocaleString(undefined, {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals,
      });

  return (
    <span className={`animated-counter ${className}`} style={{ willChange: 'contents' }}>
      {prefix}{formatted}{suffix}
    </span>
  );
};

export default AnimatedCounter;
